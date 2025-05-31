select *
from (
    select to_char (
        coalesce(vm.release_timestamp, vm.timestamp),
        'YYYY-MM-DD'
      ) as date,
      title,
      video_id,
      encode (sha256(video_id::bytea), 'hex') as video_id_sha256,
      encode (sha256(channel_id::bytea), 'hex') as channel_id_sha256
    from video_metadata vm
  )
where channel_id_sha256 in (
    '6a1fd4136d7bbd49aa9b2d6e0e9949e36cdb92c6ea4ab6f0b34d0eb648737294'
  )
order by date desc
