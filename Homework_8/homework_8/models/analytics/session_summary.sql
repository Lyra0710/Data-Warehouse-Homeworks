SELECT u.userId, u.sessionId, u.channel, st.ts
FROM {{ ref('user_session_channel_cleaned') }} u
JOIN {{ ref('session_timestamp_cleaned') }} st
ON u.sessionId = st.sessionId