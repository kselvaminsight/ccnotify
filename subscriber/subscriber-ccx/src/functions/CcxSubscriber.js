const { app } = require('@azure/functions');
const { EventGridReceiverClient } = require('@azure/eventgrid-namespaces');
const { AzureKeyCredential } = require('@azure/core-auth');
const { DBSQLClient } = require('@databricks/sql');
const crypto = require('crypto');
const { CosmosClient } = require("@azure/cosmos");

const cosmosendpoint = process.env.COSMOS_ENDPOINT; // e.g., "https://<your-account>://"
const cosmoskey = process.env.COSMOS_KEY;
const cosmosDbclient = new CosmosClient({ endpoint: cosmosendpoint, key: cosmoskey });
const maxEvents = process.env.MAX_EVENTS; // Maximum number of events to pull from Event Grid per invocation
const ackBatchSize = 10;
const customerParallelism = Number(process.env.CUSTOMER_PARALLELISM || 5);

const cosmosdatabaseId = process.env.COSMOS_DATABASE_ID;
const cosmoscontainerId = process.env.COSMOS_CONTAINER_ID;

// ---------------------------------------------------------------------------
// Databricks helpers
// ---------------------------------------------------------------------------

const connectOptions = {
  host: process.env.DATABRICKS_HOST,
  path: process.env.DATABRICKS_HTTP_PATH,
  authType: 'databricks-oauth', // Instructs driver to use OAuth M2M
  oauthClientId: process.env.DATABRICKS_CLIENT_ID,
  azureTenantId: process.env.DATABRICKS_TENANT_ID,
  oauthClientSecret: process.env.DATABRICKS_CLIENT_SECRET,
};

function escapeSqlLiteral(value) {
    return String(value || '').replace(/'/g, "''");
}


function generateUniqueKey(payload) {
    const relatedIds = Array.isArray(payload.relatedEntityId) ? payload.relatedEntityId : [];
    const sortedEntities = [...relatedIds].sort().join(',');
    const baseString = `entities:${sortedEntities}|freq:${payload.frequencyDays}|user:${payload.userId}|type:${payload.eventType}`;
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

/**
 * Look up customer rows in Databricks by b2bunit id.
 * Returns a unique list of customer objects with `id` and `soldToID`.
 */
async function lookupCustomersByB2BUnitId(b2bunitId, context, session) {

    if (!session) {
        throw new Error('Databricks session is required for customer lookup.');
    }

    const query = `SELECT  id, soldToID FROM tst_affiliate.gold.ccx_customer WHERE soldToID = '${escapeSqlLiteral(b2bunitId)}'`;
    context.log(`Constructed customer lookup query: ${query}`);

    let operation;

    try {
        operation = await session.executeStatement(query, { runAsync: false });
        const rows = await operation.fetchAll();

        const customers = Object.values(
            rows
                .filter((row) => row.id)
                .reduce((acc, row) => {
                    const key = `${row.id}::${row.soldToID || ''}`;
                    if (!acc[key]) {
                        acc[key] = {
                            id: row.id,
                            soldToID: row.soldToID
                        };
                    }
                    return acc;
                }, {})
        );

        context.log(`Customer lookup matched ${customers.length} unique customer(s) for B2B unit ID ${b2bunitId}.`);
        return customers;
    } finally {
        if (operation) await operation.close();
    }
}


// Initialize the client once outside the invocation handler to reuse TCP connections
const endpoint = process.env.EVENT_GRID_ENDPOINT;
const key = process.env.EVENT_GRID_KEY;
const topicName = process.env.TOPIC_NAME;
const subscriptionName = process.env.SUBSCRIPTION_NAME;

const client = new EventGridReceiverClient(
    endpoint,
    new AzureKeyCredential(key),
    topicName,
    subscriptionName
);

app.timer('pullEventsTimer', {
    schedule: '%NOTIFICATION_CRON_SCHEDULE%', // Executes exactly once every 10 minutes
    runOnStartup: false,
    handler: async (myTimer, context) => {
        const executionId = context.invocationId;
        context.log(`Timer triggered. Starting poll for up to ${maxEvents} events... Execution ID: ${executionId}`);
        const startTime = new Date();
        context.log(`Function started at:  ${startTime.toISOString()} Execution ID: ${executionId}`);
        try {

            // Strictly pull a max of 100 events.
            // maxWaitTimeInSeconds ensures the function doesn't sit idle for too long if queue is empty.
            const response = await client.receiveEvents({
                maxEvents: Number(maxEvents),
                maxWaitTimeInSeconds: 10
            });

            // DIAGNOSTIC LOG: Print the exact structural response from Azure
            context.log("RAW RESPONSE OBJECT:", JSON.stringify(response));

            // FIX: Look inside response.details instead of response.value
            if (!response.details || response.details.length === 0) {
                context.log('No events found in this minute slot.');
                return;
            }

            context.log(`Retrieved ${response.details.length} events. Forwarding sequentially...`);
            const databricksClient = new DBSQLClient();
            let databricksSession;

            try {
                await databricksClient.connect(connectOptions);
                databricksSession = await databricksClient.openSession();

                const lockTokens = [];

                for (const detail of response.details) {
                    try {
                        // IMPLEMENTATION TASK: Send your event payload to your target endpoint here
                        // Track token only if forwarding succeeds

                            const cosmosDbcontainer = cosmosDbclient.database(cosmosdatabaseId).container(cosmoscontainerId);

                                // 2. Extract b2bunitId, then look up customer list in Databricks
                                const b2bunitId = detail.event.data.b2bunit;
                                const customers = await lookupCustomersByB2BUnitId(b2bunitId, context, databricksSession);
                                const customersToProcess = customers.length > 0
                                    ? customers
                                    : [{ id: detail.event.data.customer || detail.event.data.soldToID || b2bunitId, soldToID: detail.event.data.soldToID || null, isFallback: true }];
                                if (customers.length === 0) {
                                    context.log(`No matching customers found for B2B unit ID ${b2bunitId} — forwarding event once using fallback customer.`);
                                }

                                // 3. Define external destination URL and prepare Basic auth once
                                const targetUrl = process.env.TARGET_API_URL;
                                const basicAuthUsername = process.env.BASIC_AUTH_USERNAME;
                                const basicAuthPassword = process.env.BASIC_AUTH_PASSWORD;
                                if (!basicAuthUsername || !basicAuthPassword) {
                                    throw new Error('Missing Basic auth configuration. Required: BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD');
                                }
                                const basicAuthToken = Buffer.from(`${basicAuthUsername}:${basicAuthPassword}`).toString('base64');

                                let forwardedCount = 0;
                                let duplicateCount = 0;
                                let failedCount = 0;

                                const dateObject = new Date(detail.event.data.timeGenerated);
                                const dateISOString = dateObject.toISOString().substring(0, 19) + 'Z'; // Ensures the format is YYYY-MM-DDTHH:mm:ssZ
                                const workerCount = Math.max(1, Math.min(customerParallelism, customersToProcess.length));
                                let customerIndex = 0;

                                const processCustomer = async (customer) => {
                                    // 5. Build and forward one payload per matched customer
                                    const payload = {
                                        notificationId: detail.event.id,
                                        userId: customer.id,
                                        eventType: detail.event.type,
                                        relatedEntityId: detail.event.data.subscriptionIds,
                                        frequencyDays: Number(detail.event.data.frequency),
                                        createdAt: dateISOString,
                                        vendor: detail.event.data.vendor
                                    };
                                    const uniqueKey = generateUniqueKey(payload);

                                    try {
                                        const trackingDoc = {
                                            id: uniqueKey, // Cosmos DB enforces uniqueness on 'id' per partition
                                            eventType: detail.event.eventType,
                                            userId: customer.id,
                                            subscriptionId: detail.event.data.subscriptionIds,
                                            processedAt: new Date().toISOString()
                                        };

                                        // 3. Perform a blind, atomic insert
                                        await cosmosDbcontainer.items.create(trackingDoc);

                                        const data = JSON.stringify(payload);

                                        context.log(`Prepared data for forwarding customer ${customer.id}: ${data}`);
                                        const targetResponse = await fetch(targetUrl, {
                                            method: 'POST',
                                            headers: {
                                                'Content-Type': 'application/json',
                                                'Authorization': `Basic ${basicAuthToken}`
                                            },
                                            body: data
                                        });

                                        // 6. Handle each destination response
                                        if (!targetResponse.ok) {
                                            const errorText = await targetResponse.text();
                                            await cosmosDbcontainer.item(trackingDoc.id, trackingDoc.id).delete();
                                            failedCount += 1;
                                            context.error(
                                                `Failed to forward event for customer ${customer.id}. Payload: ${data}. Target status: ${targetResponse.status}. Error: ${errorText}`
                                            );
                                            return;
                                        }

                                        forwardedCount += 1;
                                        context.log(
                                            `Successfully forwarded event for customer ${customer.id}. Payload: ${data}. Target status: ${targetResponse.status}`
                                        );
                                    } catch (error) {
                                        // Catch native Cosmos DB "Conflict" error code (409) and continue loop
                                        if (isCosmosConflict(error)) {
                                            duplicateCount += 1;
                                            context.warn(`Duplicate event blocked for customer ${customer.id}. Hash: ${uniqueKey}`);
                                            return;
                                        }

                                        failedCount += 1;
                                        const sc = error?.statusCode ?? error?.code ?? error?.status ?? 'n/a';
                                        context.error(`Error while processing customer ${customer.id}: status=${sc}, message=${error.message}`);
                                    }
                                };

                                const worker = async () => {
                                    while (true) {
                                        const currentIndex = customerIndex;
                                        customerIndex += 1;

                                        if (currentIndex >= customersToProcess.length) {
                                            return;
                                        }

                                        await processCustomer(customersToProcess[currentIndex]);
                                    }
                                };

                                await Promise.all(Array.from({ length: workerCount }, () => worker()));

                                context.log(`Processing summary for event ${detail.event.id}: attempted=${customersToProcess.length}, forwarded=${forwardedCount}, duplicates=${duplicateCount}, failed=${failedCount}`);

                                if (failedCount === 0 && detail.brokerProperties && detail.brokerProperties.lockToken) {
                                    lockTokens.push(detail.brokerProperties.lockToken);

                                    if (lockTokens.length >= ackBatchSize) {
                                        await client.acknowledgeEvents(lockTokens);
                                        context.log(`Successfully acknowledged ${lockTokens.length} events.`);
                                        lockTokens.length = 0;
                                    }
                                }
                        } catch (eventError) {
                        context.error(`Failed to process event ${detail.event.id}:`, eventError);
                        // Do not add token to lockTokens.
                        // The event lock will naturally expire, and it will be re-delivered on the next minute cycle.
                    }
                }
                // Acknowledge any remaining successful events to remove them from Event Grid Namespace
                if (lockTokens.length > 0) {
                    await client.acknowledgeEvents(lockTokens);
                    context.log(`Successfully acknowledged ${lockTokens.length} events.`);
                }
            } finally {
                if (databricksSession) await databricksSession.close();
                await databricksClient.close();
            }
        } catch (error) {
            context.error('Fatal error during Event Grid pull operation:', error);
        }
        const completeTime = new Date();
        const duration = completeTime - startTime;
        context.log(`Function completed at: ${completeTime.toISOString()} Execution ID: ${executionId}`);
        context.log(`Total execution duration: ${duration}ms Execution ID: ${executionId}`);
    }
});
