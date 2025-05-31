select vm.channel_name,
  to_char(
    max(coalesce(vm.release_timestamp, vm.timestamp)),
    'YYYY-MM-DD'
  ) as latest_release_timestamp,
  encode(sha256(vm.channel_id::bytea), 'hex') as channel_id_sha256,
  vm.channel_id
from video_metadata vm
group by vm.channel_name,
  vm.channel_id
order by latest_release_timestamp desc
