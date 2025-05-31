select to_char (
    coalesce(vm.release_timestamp, vm.timestamp),
    'YYYY-MM-DD'
  ) as date,
  title,
  video_id
from video_metadata vm
where video_id in (
    '6qRwsGJXV2k'
    '7tq1YGVdPx4',
    '7xpy9DhEdDo',
    'DDMh3FTUAGA',
    'dZSuq11ChGk',
    'eGwpa2OmQMY',
    'GQ89hSaSff4',
    'I6xrkDABPw4',
    'J8Da7DgGgtM',
    'N1dFWp2rdvo',
    'QYlDf09X4FE',
    'scnoaETm-Bc',
    'teWSxSxIws0',
    'ttayh3dZXTk',
    'vh2Kb-DFkY0',
    'YT0AahfOhYg'
  )
order by date desc
