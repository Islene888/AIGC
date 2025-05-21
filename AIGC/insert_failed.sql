-- 2025-05-01

    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT * FROM (
      SELECT
        b.event_date,
        m.tag,
        m.bot_type,
        SUM(b.total_chat_events) AS total_chat_events,
        SUM(b.chat_user_count) AS total_chat_user_count,
        ROUND(SUM(b.total_chat_events) * 1.0 / NULLIF(SUM(b.chat_user_count), 0), 2) AS chat_depth_user
      FROM (
        SELECT
          c.event_date,
          c.prompt_id,
          COUNT(*) AS total_chat_events,
          COUNT(DISTINCT c.user_id) AS chat_user_count
        FROM flow_event_info.tbl_app_event_chat_send c
        WHERE c.event_date = '2025-05-01'
          AND (c.method IS NULL OR c.method != 'regenerate')
        GROUP BY c.event_date, c.prompt_id
      ) b
      JOIN (
        SELECT
          t.prompt_id,
          t.event_date,
          t.tag,
          CASE 
            WHEN a.prompt_id IS NOT NULL THEN 'AIGC'
            ELSE 'AllBots'
          END AS bot_type
        FROM tbl_bot_tags_exploded_daily t
        LEFT JOIN AIGC_prompt_tag_with_v5 a
          ON t.prompt_id = a.prompt_id
        WHERE t.event_date = '2025-05-01'
      ) m
      ON b.prompt_id = m.prompt_id AND b.event_date = m.event_date
      GROUP BY b.event_date, m.tag, m.bot_type
      HAVING SUM(b.total_chat_events) > 0
    ) tmp;
    

-- 2025-05-02

    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT * FROM (
      SELECT
        b.event_date,
        m.tag,
        m.bot_type,
        SUM(b.total_chat_events) AS total_chat_events,
        SUM(b.chat_user_count) AS total_chat_user_count,
        ROUND(SUM(b.total_chat_events) * 1.0 / NULLIF(SUM(b.chat_user_count), 0), 2) AS chat_depth_user
      FROM (
        SELECT
          c.event_date,
          c.prompt_id,
          COUNT(*) AS total_chat_events,
          COUNT(DISTINCT c.user_id) AS chat_user_count
        FROM flow_event_info.tbl_app_event_chat_send c
        WHERE c.event_date = '2025-05-02'
          AND (c.method IS NULL OR c.method != 'regenerate')
        GROUP BY c.event_date, c.prompt_id
      ) b
      JOIN (
        SELECT
          t.prompt_id,
          t.event_date,
          t.tag,
          CASE 
            WHEN a.prompt_id IS NOT NULL THEN 'AIGC'
            ELSE 'AllBots'
          END AS bot_type
        FROM tbl_bot_tags_exploded_daily t
        LEFT JOIN AIGC_prompt_tag_with_v5 a
          ON t.prompt_id = a.prompt_id
        WHERE t.event_date = '2025-05-02'
      ) m
      ON b.prompt_id = m.prompt_id AND b.event_date = m.event_date
      GROUP BY b.event_date, m.tag, m.bot_type
      HAVING SUM(b.total_chat_events) > 0
    ) tmp;
    

-- 2025-04-21

    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT * FROM (
      SELECT
        b.event_date,
        m.tag,
        m.bot_type,
        SUM(b.total_chat_events) AS total_chat_events,
        SUM(b.chat_user_count) AS total_chat_user_count,
        ROUND(SUM(b.total_chat_events) * 1.0 / NULLIF(SUM(b.chat_user_count), 0), 2) AS chat_depth_user
      FROM (
        SELECT
          c.event_date,
          c.prompt_id,
          COUNT(*) AS total_chat_events,
          COUNT(DISTINCT c.user_id) AS chat_user_count
        FROM flow_event_info.tbl_app_event_chat_send c
        WHERE c.event_date = '2025-04-21'
          AND (c.method IS NULL OR c.method != 'regenerate')
        GROUP BY c.event_date, c.prompt_id
      ) b
      JOIN (
        SELECT
          t.prompt_id,
          t.event_date,
          t.tag,
          CASE 
            WHEN a.prompt_id IS NOT NULL THEN 'AIGC'
            ELSE 'AllBots'
          END AS bot_type
        FROM tbl_bot_tags_exploded_daily t
        LEFT JOIN AIGC_prompt_tag_with_v5 a
          ON t.prompt_id = a.prompt_id
        WHERE t.event_date = '2025-04-21'
      ) m
      ON b.prompt_id = m.prompt_id AND b.event_date = m.event_date
      GROUP BY b.event_date, m.tag, m.bot_type
      HAVING SUM(b.total_chat_events) > 0
    ) tmp;
    

-- 2025-05-01

    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT * FROM (
      SELECT
        b.event_date,
        m.tag,
        m.bot_type,
        SUM(b.total_chat_events) AS total_chat_events,
        SUM(b.chat_user_count) AS total_chat_user_count,
        ROUND(SUM(b.total_chat_events) * 1.0 / NULLIF(SUM(b.chat_user_count), 0), 2) AS chat_depth_user
      FROM (
        SELECT
          c.event_date,
          c.prompt_id,
          COUNT(*) AS total_chat_events,
          COUNT(DISTINCT c.user_id) AS chat_user_count
        FROM flow_event_info.tbl_app_event_chat_send c
        WHERE c.event_date = '2025-05-01'
          AND (c.method IS NULL OR c.method != 'regenerate')
        GROUP BY c.event_date, c.prompt_id
      ) b
      JOIN (
        SELECT
          t.prompt_id,
          t.event_date,
          t.tag,
          CASE 
            WHEN a.prompt_id IS NOT NULL THEN 'AIGC'
            ELSE 'AllBots'
          END AS bot_type
        FROM tbl_bot_tags_exploded_daily t
        LEFT JOIN AIGC_prompt_tag_with_v5 a
          ON t.prompt_id = a.prompt_id
        WHERE t.event_date = '2025-05-01'
      ) m
      ON b.prompt_id = m.prompt_id AND b.event_date = m.event_date
      GROUP BY b.event_date, m.tag, m.bot_type
      HAVING SUM(b.total_chat_events) > 0
    ) tmp;
    

-- 2025-05-02

    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT * FROM (
      SELECT
        b.event_date,
        m.tag,
        m.bot_type,
        SUM(b.total_chat_events) AS total_chat_events,
        SUM(b.chat_user_count) AS total_chat_user_count,
        ROUND(SUM(b.total_chat_events) * 1.0 / NULLIF(SUM(b.chat_user_count), 0), 2) AS chat_depth_user
      FROM (
        SELECT
          c.event_date,
          c.prompt_id,
          COUNT(*) AS total_chat_events,
          COUNT(DISTINCT c.user_id) AS chat_user_count
        FROM flow_event_info.tbl_app_event_chat_send c
        WHERE c.event_date = '2025-05-02'
          AND (c.method IS NULL OR c.method != 'regenerate')
        GROUP BY c.event_date, c.prompt_id
      ) b
      JOIN (
        SELECT
          t.prompt_id,
          t.event_date,
          t.tag,
          CASE 
            WHEN a.prompt_id IS NOT NULL THEN 'AIGC'
            ELSE 'AllBots'
          END AS bot_type
        FROM tbl_bot_tags_exploded_daily t
        LEFT JOIN AIGC_prompt_tag_with_v5 a
          ON t.prompt_id = a.prompt_id
        WHERE t.event_date = '2025-05-02'
      ) m
      ON b.prompt_id = m.prompt_id AND b.event_date = m.event_date
      GROUP BY b.event_date, m.tag, m.bot_type
      HAVING SUM(b.total_chat_events) > 0
    ) tmp;
    

-- 2025-05-03

    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT * FROM (
      SELECT
        b.event_date,
        m.tag,
        m.bot_type,
        SUM(b.total_chat_events) AS total_chat_events,
        SUM(b.chat_user_count) AS total_chat_user_count,
        ROUND(SUM(b.total_chat_events) * 1.0 / NULLIF(SUM(b.chat_user_count), 0), 2) AS chat_depth_user
      FROM (
        SELECT
          c.event_date,
          c.prompt_id,
          COUNT(*) AS total_chat_events,
          COUNT(DISTINCT c.user_id) AS chat_user_count
        FROM flow_event_info.tbl_app_event_chat_send c
        WHERE c.event_date = '2025-05-03'
          AND (c.method IS NULL OR c.method != 'regenerate')
        GROUP BY c.event_date, c.prompt_id
      ) b
      JOIN (
        SELECT
          t.prompt_id,
          t.event_date,
          t.tag,
          CASE 
            WHEN a.prompt_id IS NOT NULL THEN 'AIGC'
            ELSE 'AllBots'
          END AS bot_type
        FROM tbl_bot_tags_exploded_daily t
        LEFT JOIN AIGC_prompt_tag_with_v5 a
          ON t.prompt_id = a.prompt_id
        WHERE t.event_date = '2025-05-03'
      ) m
      ON b.prompt_id = m.prompt_id AND b.event_date = m.event_date
      GROUP BY b.event_date, m.tag, m.bot_type
      HAVING SUM(b.total_chat_events) > 0
    ) tmp;
    

-- 2025-04-16

   -- 行为按天，tag 静态
SELECT
  '2025-04-16' AS event_date,
  tag,
  bot_type,
  SUM(total_chat_events) AS total_chat_events,
  SUM(chat_user_count) AS total_chat_user_count,
  ROUND(SUM(total_chat_events) * 1.0 / NULLIF(SUM(chat_user_count), 0), 2) AS chat_depth_user
FROM (
  SELECT
    c.prompt_id,
    t.tag,
    CASE WHEN a.prompt_id IS NOT NULL THEN 'AIGC' ELSE 'AllBots' END AS bot_type,
    COUNT(*) AS total_chat_events,
    COUNT(DISTINCT c.user_id) AS chat_user_count
  FROM flow_event_info.tbl_app_event_chat_send c
  LEFT JOIN AIGC_prompt_tag_with_v5 t ON c.prompt_id = t.prompt_id
  LEFT JOIN AIGC_prompt_tag_with_v5 a ON c.prompt_id = a.prompt_id
  WHERE c.event_date = '2025-04-16'
    AND (c.method IS NULL OR c.method != 'regenerate')
  GROUP BY c.prompt_id, t.tag, bot_type
) tmp
GROUP BY tag, bot_type;

    

-- 2025-04-16

    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT
      '2025-04-16' AS event_date,
      t.tag,
      CASE WHEN t.prompt_id IS NOT NULL THEN 'AIGC' ELSE 'AllBots' END AS bot_type,
      SUM(total_chat_events) AS total_chat_events,
      SUM(chat_user_count) AS total_chat_user_count,
      ROUND(SUM(total_chat_events) * 1.0 / NULLIF(SUM(chat_user_count), 0), 2) AS chat_depth_user
    FROM (
      SELECT
        c.prompt_id,
        COUNT(*) AS total_chat_events,
        COUNT(DISTINCT c.user_id) AS chat_user_count
      FROM flow_event_info.tbl_app_event_chat_send c
      WHERE c.event_date = '2025-04-16'
        AND (c.method IS NULL OR c.method != 'regenerate')
      GROUP BY c.prompt_id
    ) b
    LEFT JOIN AIGC_prompt_tag_with_v5 t ON b.prompt_id = t.prompt_id
    GROUP BY t.tag, bot_type;
    

