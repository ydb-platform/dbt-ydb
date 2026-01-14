-- Normalized view of raw clicks
-- Purpose:
--   - Rename columns to snake_case
--   - Clean strings: remove leading/trailing whitespace and convert empty to NULL
--   - Cast boolean flags (Int16 0/1) to Bool
--   - Prepare clean foundation for staging models
--
-- Uses macro `clean_string()` to handle YDB's lack of TRIM/NULLIF.
-- All string fields are CAST to Utf8 before cleaning (except where String is intentional).

SELECT

    last_modified_at           AS last_modified_at,

    -- ===================================================================
    -- IDENTITY & TIME
    -- ===================================================================
    WatchID                    AS event_id,
    UserID                     AS user_id,
    CounterID                  AS counter_id,
    FUniqID                    AS fingerprint_id,

    EventTime                  AS event_time,
    EventDate                  AS event_date,
    ClientEventTime            AS client_event_time,
    LocalEventTime             AS local_event_time,

    -- ===================================================================
    -- TRAFFIC & MARKETING
    -- ===================================================================
    {{ clean_string('UTMSource') }}       AS utm_source,
    {{ clean_string('UTMMedium') }}       AS utm_medium,
    {{ clean_string('UTMCampaign') }}     AS utm_campaign,
    {{ clean_string('UTMContent') }}      AS utm_content,
    {{ clean_string('UTMTerm') }}         AS utm_term,

    TraficSourceID                        AS traffic_source_id,
    SearchEngineID                        AS search_engine_id,
    {{ clean_string('SearchPhrase') }}   AS search_phrase,
    AdvEngineID                           AS adv_engine_id,
    {{ clean_string('Referer') }}        AS referrer_url,

    {{ clean_string('OpenstatServiceName') }} AS openstat_service_name,
    {{ clean_string('OpenstatCampaignID') }}  AS openstat_campaign_id,
    {{ clean_string('OpenstatAdID') }}       AS openstat_ad_id,
    {{ clean_string('OpenstatSourceID') }}   AS openstat_source_id,

    -- ===================================================================
    -- USER & DEVICE
    -- ===================================================================
    UserAgent                             AS user_agent_id,
    UserAgentMajor                        AS user_agent_major,
    UserAgentMinor                        AS user_agent_minor,
    OS                                    AS os_id,
    CAST(IsMobile AS Bool)                AS is_mobile,
    MobilePhone                           AS mobile_phone_id,
    {{ clean_string('MobilePhoneModel') }} AS mobile_phone_model,

    ResolutionWidth                       AS screen_width,
    ResolutionHeight                      AS screen_height,
    ResolutionDepth                       AS screen_color_depth,
    WindowClientWidth                     AS window_width,
    WindowClientHeight                    AS window_height,

    ClientIP                              AS client_ip,
    RegionID                              AS region_id,
    ClientTimeZone                        AS client_timezone_offset,
    {{ clean_string('BrowserLanguage') }} AS browser_language,
    {{ clean_string('BrowserCountry') }}  AS browser_country,

    -- ===================================================================
    -- PAGE & BEHAVIOR
    -- ===================================================================
    {{ clean_string('URL') }}             AS page_url,
    {{ clean_string('Title') }}           AS page_title,
    HTTPError                             AS http_error_code,

    CAST(IsLink AS Bool)                  AS is_link_click,
    CAST(IsDownload AS Bool)              AS is_download,
    CAST(IsNotBounce AS Bool)             AS is_not_bounce,
    CAST(GoodEvent AS Bool)               AS is_good_event,

    -- ===================================================================
    -- USER PROFILE
    -- ===================================================================
    Age                                   AS age,
    Sex                                   AS sex,
    Income                                AS income_level,
    Interests                             AS interests_bitmask,
    Robotness                             AS robot_score,

    -- ===================================================================
    -- TECHNICAL & MISC
    -- ===================================================================
    {{ clean_string('Params') }}             AS additional_params,
    {{ clean_string('OriginalURL') }}        AS original_url,
    {{ clean_string('PageCharset') }}        AS page_charset,
    CodeVersion                              AS tracking_code_version,
    HID                                      AS hardware_id,
    CLID                                     AS click_id,
    WindowName                               AS window_name,
    OpenerName                               AS opener_window_name,
    HistoryLength                            AS browser_history_length,
    {{ clean_string('HitColor') }}           AS screen_color_code,

    {{ clean_string('ParamOrderID') }}       AS ecommerce_order_id,
    ParamPrice                               AS product_price,
    {{ clean_string('ParamCurrency') }}      AS currency_code,
    ParamCurrencyID                          AS currency_id,

    {{ clean_string('FromTag') }}            AS from_tag,
    RefererHash                              AS referrer_url_hash,
    URLHash                                  AS page_url_hash,
    CAST(WithHash AS Bool)                   AS has_url_fragment,

    CAST(IsOldCounter AS Bool)               AS is_old_counter,
    CAST(IsParameter AS Bool)                AS is_parameterized,
    CAST(IsEvent AS Bool)                    AS is_custom_event,
    CAST(DontCountHits AS Bool)              AS is_ignored_hit,
    CAST(IsArtifical AS Bool)                AS is_artificial,
    CAST(IsRefresh AS Bool)                  AS is_refresh,

    CAST(CookieEnable AS Bool)               AS cookies_enabled,
    CAST(JavascriptEnable AS Bool)           AS javascript_enabled,
    CAST(JavaEnable AS Bool)                 AS java_enabled,

    FlashMajor                               AS flash_major_version,
    FlashMinor                               AS flash_minor_version,
    {{ clean_string('FlashMinor2') }}        AS flash_patch_version,

    SilverlightVersion1                      AS silverlight_version_1,
    SilverlightVersion2                      AS silverlight_version_2,
    SilverlightVersion3                      AS silverlight_version_3,
    SilverlightVersion4                      AS silverlight_version_4,

    {{ clean_string('SocialNetwork') }}      AS social_network,
    {{ clean_string('SocialAction') }}       AS social_action,
    SocialSourceNetworkID                    AS social_source_network_id,
    {{ clean_string('SocialSourcePage') }}   AS social_source_page,

    SendTiming                               AS send_timing,
    DNSTiming                                AS dns_timing,
    ConnectTiming                            AS connect_timing,
    ResponseStartTiming                      AS response_start_timing,
    ResponseEndTiming                        AS response_end_timing,
    FetchTiming                              AS fetch_timing,

    IPNetworkID                              AS ip_network_id,
    RemoteIP                                 AS remote_ip,

    RefererCategoryID                        AS referrer_category_id,
    RefererRegionID                          AS referrer_region_id,
    URLCategoryID                            AS page_category_id,
    URLRegionID                              AS page_region_id

FROM {{ ref('raw_clicks') }}
