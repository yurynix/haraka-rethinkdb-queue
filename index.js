const rethinkdbdash = require('rethinkdbdash');

exports.register = function register() {
	const plugin = this;

	plugin.register_hook('init_master',  'init_rethink_db');
	plugin.register_hook('init_child',   'init_rethink_db');
	plugin.register_hook('queue', 'save_msg_to_db');
};

exports.init_rethink_db = next => {
	if (!server.notes.r) server.notes.r = rethinkdbdash({
		'db': 'mail',
		'servers': [
			{host: '172.17.0.2', port: 28015}
		]
	});
	next();
};

exports._createTable = (db, table) => {
	const r = server.notes.r;

	return r.branch(
		r.dbList().contains(db),
		{dbs_created: 1},
		r.dbCreate(db)
	).do(() => {
		return r.db(db).tableCreate(table);
	}).run();
};

exports._getMessage = function _getMessage(txn) {
   	const headers = {};
	['From', 'To', 'Subject'].forEach(function (h) {
		const hdr_val = txn.header.get_decoded(h);
		if (!hdr_val) return;
		headers[h] = hdr_val;
	});

	return new Promise(function resolver(resolve, reject) {
		const stream = txn.message_stream;
		stream.get_data(function (msg_buffer) {
			resolve({
				'transactionId': txn.uuid,
				'from': txn.mail_from,
				'to': txn.rcpt_to,
				'headers': headers,
				'message': msg_buffer
			});
		});
	});
};

exports.save_msg_to_db = function save_msg_to_db(next, connection) {
	const plugin = this;
	const txn = connection.transaction;
	const r = server.notes.r;

	plugin._getMessage(txn).then(msg => {
		return r.table('messages').insert(msg).run();
	}).then(() => {
		next(OK, "Queued!");
	}, err => {
		next();
	});
};
