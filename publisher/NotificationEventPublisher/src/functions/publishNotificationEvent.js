import { app } from '@azure/functions';
import { EventGridSenderClient, AzureKeyCredential } from '@azure/eventgrid-namespaces';
import { DBSQLClient } from '@databricks/sql';
import crypto from 'crypto';
import { CosmosClient } from '@azure/cosmos';
import fs from 'fs';
import path from 'path';


const endpoint = process.env["EVENT_GRID_NAMESPACES_ENDPOINT"];
const key = process.env["EVENT_GRID_NAMESPACES_KEY"];
const topicName = process.env["TOPIC_NAME"];
const cloudEventType = process.env["CLOUD_EVENT_TYPE"];
const cloudEventSource = process.env["CLOUD_EVENT_SOURCE"];
const cloudEventVendor = process.env["CLOUD_EVENT_VENDOR"];
const subscriptionQueryFile = process.env["SUBSCRIPTION_QUERY_FILE"];
const subscriptionQueryTemplatePath = path.resolve(process.cwd(), subscriptionQueryFile);
const subscriptionQueryTemplate = fs.readFileSync(subscriptionQueryTemplatePath, 'utf-8');
const cosmosendpoint = process.env.COSMOS_ENDPOINT; // e.g., "https://<your-account>://"
const cosmoskey = process.env.COSMOS_KEY;
const cosmosDbclient = new CosmosClient({ endpoint: cosmosendpoint, key: cosmoskey });
const cosmosdatabaseId = process.env.COSMOS_DATABASE_ID;
const cosmoscontainerId = process.env.COSMOS_CONTAINER_ID;

function generateUniqueKey(eventPayload) {
    const originKey = 'origin';
    const publisherValue = 'Publisher';
    const subscriptionIds = Array.isArray(eventPayload.subscriptionIds) ? [...eventPayload.subscriptionIds] : [];
    const sortedSubscriptionIds = subscriptionIds.map(id => String(id)).sort().join(',');
    const baseString = [
        `type:${eventPayload.eventType}`,
        `vendor:${eventPayload.vendor}`,
        `${originKey}:${publisherValue}`,
        `soldTo:${eventPayload.soldToId}`,
        `site:${eventPayload.siteId}`,
        `frequency:${eventPayload.frequency}`,
        `subscriptions:${sortedSubscriptionIds}`
    ].join('|');

    return crypto.createHash('sha256').update(baseString).digest('hex');
}

function isCosmosConflict(error) {
    const statusCode = Number(error?.statusCode ?? error?.code ?? error?.status);
    const message = String(error?.message || '');

    if (statusCode === 409) {
        return true;
    }

    if (/\bStatusCode\s*:\s*409\b/i.test(message)) {
        return true;
    }

    if (/already exists in the system/i.test(message)) {
        return true;
    }

    return false;
}


const connectOptions = {
  host: process.env.DATABRICKS_HOST,
  path: process.env.DATABRICKS_HTTP_PATH,
  authType: 'databricks-oauth', // Instructs driver to use OAuth M2M
  oauthClientId: process.env.DATABRICKS_CLIENT_ID,
  azureTenantId: process.env.DATABRICKS_TENANT_ID,
  oauthClientSecret: process.env.DATABRICKS_CLIENT_SECRET,
};

const senderClient = new EventGridSenderClient(endpoint, new AzureKeyCredential(key), topicName);

function buildSubscriptionQuery(caseConditions, caseINConditions) {
    return subscriptionQueryTemplate
        .replace('{{CASE_CONDITIONS}}', caseConditions)
        .replace('{{CASE_IN_CONDITIONS}}', caseINConditions);
}


app.timer('publishEvents', {
    schedule: '%NOTIFICATION_CRON_SCHEDULE%',
    handler: async (myTimer, context) => {
        
        context.log('Timer function processed request.');
        const sqlClient = new DBSQLClient();
        let session;
        let operation;
        let totalEventsSent = 0;
        let duplicateEventsSkipped = 0;
        try {

            await sqlClient.connect(connectOptions);

            session = await sqlClient.openSession();
            /*
             1. Fetch enabled frequencies
             const frequencyQuery = `
                 SELECT frequency
                 FROM notification_configuration
                 WHERE frequencyEnabled = 'TRUE' AND eventType='subscription_auto_renewing'
                 GROUP BY frequency
             `;
             operation = await session.executeStatement(frequencyQuery, { runAsync: false });
             const frequencyRows = await operation.fetchAll();*/
            const frequencyRows= [30,60,90];
            if (!frequencyRows || frequencyRows.length === 0) {
                context.log('No enabled frequencies found.');
                return { status: 200, body: '0 event(s) sent!' };
            }
            const frequencies = frequencyRows
                .map(row => (typeof row === 'number' ? row : row.frequency))
                .filter(f => Number.isFinite(f));

            if (frequencies.length === 0) {
                context.log('No valid frequencies found.');
                return { status: 200, body: '0 event(s) sent!' };
            }
            const dateColumn = String(cloudEventVendor || '').toLowerCase() === 'adobe' ? 'anniversaryDate' : 'endDate';

            const caseConditions = frequencies
                .sort((a, b) => a - b)
                .map(f => `WHEN ${dateColumn} = DATE_ADD(CURRENT_DATE(), ${f}) THEN ${f}`)
                .join('\n                        ');

            const caseINConditions = frequencies
            .sort((a, b) => a - b)
            .map(f => ` DATE_ADD(CURRENT_DATE(), ${f})`)
            .join(',\n                                        ');
            //await operation.close(); // Clean up frequency operation

            // 2. Main Subscription Aggregation Query
            const subscriptionQuery = buildSubscriptionQuery(caseConditions, caseINConditions);

            context.log('Executing streaming query on Databricks...');
            operation = await session.executeStatement(subscriptionQuery, { runAsync: false });

          
            const EVENT_GRID_BATCH_LIMIT = 1000; // Safe threshold for Event Grid
            let cloudEventBatch = [];
            const cosmosDbcontainer = cosmosDbclient.database(cosmosdatabaseId).container(cosmoscontainerId);
            // 3. Official Databricks SDK Loop using fetchChunk() and hasMoreRows()
            do {
                // Fetch next chunk data. We set maxRows to limit RAM pressure.
                const chunkRows = await operation.fetchChunk({ maxRows: 2000 });
                context.log(`Fetched chunk of ${chunkRows.length} rows from Databricks.`);
                if (chunkRows && chunkRows.length > 0) {
                    for (const chunkRow of chunkRows) {
                        const nowIso = new Date().toISOString();
                        const eventPayload = {
                            eventType: cloudEventType,
                            vendor: cloudEventVendor,
                            soldToId: chunkRow.soldToID,
                            siteId: chunkRow.siteID,
                            frequency: Number(chunkRow.expiry_window),
                            subscriptionIds: Array.isArray(chunkRow.subscription_ids) ? chunkRow.subscription_ids : [],
                            createdAt: nowIso
                        };

                        const uniqueKey = generateUniqueKey(eventPayload);
                        let isNewEvent = false;

                        try {
                            await cosmosDbcontainer.items.create({
                                id: uniqueKey,
                                uniqueKey,
                                ...eventPayload
                            });
                            isNewEvent = true;
                        } catch (error) {
                            if (isCosmosConflict(error)) {
                                duplicateEventsSkipped += 1;
                                context.log(`Duplicate event detected for key ${uniqueKey}. Skipping Event Grid publish.`);
                                continue;
                            }
                            throw error;
                        }

                        if (!isNewEvent) {
                            continue;
                        }

                        cloudEventBatch.push({
                            type: cloudEventType,
                            source: cloudEventSource,
                            subject: `/soldTo/${chunkRow.soldToID}/site/${chunkRow.siteID}`,
                            id: uniqueKey,
                            time: new Date(),
                            data: {
                                b2bunit: chunkRow.soldToID,
                                timeGenerated: nowIso,
                                frequency: chunkRow.expiry_window,
                                subscriptionIds: chunkRow.subscription_ids,
                                baseStore: chunkRow.siteID,
                                vendor: cloudEventVendor
                            },
                            specversion: "1.0",
                        });
                        
                        // Batch dispatch to Event Grid to circumvent 1000 limitation
                        if (cloudEventBatch.length >= EVENT_GRID_BATCH_LIMIT) {
                            await senderClient.sendEvents(cloudEventBatch);
                            totalEventsSent += cloudEventBatch.length;
                            context.log(`Dispatched batch of ${cloudEventBatch.length} notification events.`);
                            cloudEventBatch = []; // Flush memory pointer reference
                        }
                    }
                }
            } while (await operation.hasMoreRows());

            // Flush remaining data leftovers 
            if (cloudEventBatch.length > 0) {
                await senderClient.sendEvents(cloudEventBatch);
                totalEventsSent += cloudEventBatch.length;
                context.log(`Dispatched final batch of ${cloudEventBatch.length} notification events.`);
            }

            context.log(`Completed process. Total Events Published: ${totalEventsSent}. Duplicate Events Skipped: ${duplicateEventsSkipped}`);
            return { status: 200, body: `${totalEventsSent} event(s) sent!` };

        } catch (error) {
            context.log(`Error during orchestration processing: ${error.message}`);
            return { status: 500, body: 'Internal Server Error' };
        } finally {
            // Asynchronous connection cleanup prevents leaks
            if (operation) await operation.close();
            if (session) await session.close();
            await sqlClient.close();
        }
    }
});
