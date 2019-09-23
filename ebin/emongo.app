{application, emongo, [
  {description, "Erlang MongoDB Driver"},
  {vsn, "1.0.1"},
  {modules, [
	  emongo,
	  emongo_app,
	  emongo_bson,
	  emongo_conn,
	  emongo_packet
  ]},
  {registered, []},
  {mod, {emongo_app, []}},
  {applications, [kernel, stdlib]}
]}.
