select sessionID, ts
from {{ source('raw', 'session_timestamp') }}
where sessionID is not null
 