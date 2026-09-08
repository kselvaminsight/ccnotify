WITH CategorizedSubscriptions AS (
    SELECT 
        id,
        soldToID,
        anniversaryDate,
        siteID,
        CASE 
            {{CASE_CONDITIONS}}
        END as expiry_window
    FROM tst_affiliate.gold.ccx_adobe_subscription 
    WHERE anniversaryDate IN (
        {{CASE_IN_CONDITIONS}}
    )
    AND status IN ('active','changerequested')
    AND autorenew = 0
)
SELECT 
    expiry_window,
    soldToID,
    siteID,
    collect_list(id) as subscription_ids
FROM CategorizedSubscriptions
WHERE expiry_window IS NOT NULL
GROUP BY expiry_window, soldToID, siteID;
