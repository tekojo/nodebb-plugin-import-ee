
var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-ee]';

(function(Exporter) {

    Exporter.setup = function(config, callback) {
        Exporter.log('setup');

        // mysql db only config
        // extract them from the configs passed by the nodebb-plugin-import adapter
        var _config = {
            host: config.dbhost || config.host || 'localhost',
            user: config.dbuser || config.user || 'user',
            password: config.dbpass || config.pass || config.password || undefined,
            port: config.dbport || config.port || 3306,
            database: config.dbname || config.name || config.database || 'ee'
        };

        Exporter.config(_config);
        Exporter.config('prefix', config.prefix || config.tablePrefix || '');

        config.custom = config.custom || {};
        if (typeof config.custom === 'string') {
            try {
                config.custom = JSON.parse(config.custom)
            } catch (e) {}
        }

        Exporter.config('custom', config.custom || {
            messagesPlugin: ''
        });

        Exporter.connection = mysql.createConnection(_config);
        Exporter.connection.connect();

        callback(null, Exporter.config());
    };

    Exporter.query = function(query, callback) {
        if (!Exporter.connection) {
            var err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }
        // console.log('\n\n====QUERY====\n\n' + query + '\n');
        Exporter.connection.query(query, function(err, rows) {
            //if (rows) {
            //    console.log('returned: ' + rows.length + ' results');
            //}
            callback(err, rows)
        });
    };
    var getGroups = function(config, callback) {
        if (_.isFunction(config)) {
            callback = config;
            config = {};
        }
        callback = !_.isFunction(callback) ? noop : callback;
        if (!Exporter.connection) {
            Exporter.setup(config);
        }
        var prefix = Exporter.config('prefix');
        var query = 'SELECT '
            + prefix + 'exp_member_groups.group_id as _gid, '
            + prefix + 'exp_member_groups.group_title as _title, '
            + prefix + 'exp_member_groups.can_email_from_profile as _pmpermissions, ' // pm as in private messaging
            + prefix + 'exp_member_groups.can_admin_upload_prefs as _adminpermissions '
            + ' from ' + prefix + 'exp_member_groups ';
        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                var map = {};

                //figure out the admin group
                var max = 0, admingid;
                rows.forEach(function(row) {
                    var adminpermission = row._adminpermissions;
                    if (adminpermission == 'y') {
                        admingid = row._gid;
                    }
                });

                rows.forEach(function(row) {
                    if (row._pmpermissions == 'n') {
                        row._banned = 1;
                        row._level = 'member';
                    } else if (row._adminpermissions == 'y') {
                        row._level = row._gid === admingid ? 'administrator' : 'moderator';
                        row._banned = 0;
                    } else {
                        row._level = 'member';
                        row._banned = 0;
                    }
                    map[row._gid] = row;
                });
                // keep a copy of the users in memory here
                Exporter._groups = map;
                callback(null, map);
            });
    };

    Exporter.getUsers = function(callback) {
        return Exporter.getPaginatedUsers(0, -1, callback);
    };
    Exporter.getPaginatedUsers = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix') || '';
        var startms = +new Date();

        var query = 'SELECT '
            + prefix + 'exp_members.member_id as _uid, '
            + prefix + 'exp_members.email as _email, '
            + prefix + 'exp_members.username as _username, '
            + prefix + 'exp_members.signature as _signature, '
            + prefix + 'exp_members.join_date as _joindate, '
            + prefix + 'exp_members.url as _website, '
            + prefix + 'exp_urs_member_reward.points as _reputation '
            + 'FROM ' + prefix + 'exp_members '
            + 'JOIN ' + prefix + 'exp_urs_member_reward ON ' + prefix + 'exp_members.member_id=' + prefix + 'exp_urs_member_reward.member_id '
				+ 'WHERE exp_members.last_comment_date<>0 or exp_members.last_forum_post_date<>0 '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


        getGroups(function(err, groups) {
            Exporter.query(query,
                function(err, rows) {
                    if (err) {
                        Exporter.error(err);
                        return callback(err);
                    }

                    //normalize here
                    var map = {};
                    rows.forEach(function(row) {
                        // nbb forces signatures to be less than 150 chars
                        // keeping it HTML see https://github.com/akhoury/nodebb-plugin-import#markdown-note
                        row._signature = Exporter.truncateStr(row._signature || '', 150);

                        // from unix timestamp (s) to JS timestamp (ms)
                        row._joindate = ((row._joindate || 0) * 1000) || startms;

                        // lower case the email for consistency
                        row._email = (row._email || '').toLowerCase();
                        row._website = Exporter.validateUrl(row._website);

                        row._level = (groups[row._gid] || {})._level || '';
                        row._banned = (groups[row._gid] || {})._banned || 0;

                        map[row._uid] = row;
                    });

                    callback(null, map);
                });
        });
    };

    var supportedPlugins = {
    };

    Exporter.getMessages = function(callback) {
        return Exporter.getPaginatedMessages(0, -1, callback);
    };
    Exporter.getPaginatedMessages = function(start, limit, callback) {
        var custom = Exporter.config('custom') || {};
        custom.messagesPlugin = (custom.messagesPlugin || '').toLowerCase();

        if (supportedPlugins[custom.messagesPlugin]) {
            return supportedPlugins[custom.messagesPlugin](start, limit, callback);
        }

        callback = !_.isFunction(callback) ? noop : callback;

        var startms = +new Date();
        var prefix = Exporter.config('prefix') || '';
        var query = 'SELECT '
            + prefix + 'exp_message_data.message_id as _mid, '
            + prefix + 'exp_message_data.sender_id as _fromuid, '
            + prefix + 'exp_message_data.message_recipient as _touid, '
            + prefix + 'exp_message_data.message_body as _content, '
            + prefix + 'exp_message_data.message_date as _timestamp '
            + 'FROM ' + prefix + 'exp_message_data '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    map[row._mid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.getCategories = function(callback) {
        return Exporter.getPaginatedCategories(0, -1, callback);
    };
    Exporter.getPaginatedCategories = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();

        var query = 'SELECT '
            + prefix + 'exp_forums.forum_id as _cid, '
            + prefix + 'exp_forums.forum_name as _name, '
            + prefix + 'exp_forums.forum_description as _description, '
            + prefix + 'exp_forums.forum_order as _order '
            + 'FROM ' + prefix + 'exp_forums ' 
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._name = row._name || 'Untitled Category ';
                    row._description = row._description || 'No decsciption available';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    map[row._cid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.getTopics = function(callback) {
        return Exporter.getPaginatedTopics(0, -1, callback);
    };
    Exporter.getPaginatedTopics = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'exp_forum_topics.topic_id as _tid, '
            + prefix + 'exp_forum_topics.author_id as _uid, '
            + prefix + 'exp_forum_topics.forum_id as _cid, '
            + prefix + 'exp_forum_topics.title as _title, '
            + prefix + 'exp_forum_topics.body as _content, '
//            + prefix + 'exp_members.username as _guest, '  
            + prefix + 'exp_forum_topics.ip_address as _ip, '
            + prefix + 'exp_forum_topics.topic_date as _timestamp, '
            + prefix + 'exp_forum_topics.thread_views as _viewcount, '
            + prefix + 'exp_forum_topics.status as _open, '
//            + prefix + 'exp_forum_topics.pentry_id as _deleted, '  // not used
            + prefix + 'exp_forum_topics.sticky as _pinned '
            + 'FROM ' + prefix + 'exp_forum_topics '
//            + 'JOIN ' + prefix + 'exp_members ON ' + prefix + 'exp_forum_topics.author_id=' + prefix + 'exp_members.member_id '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    row._pinned = (row._pinned == 'y') ? 1 : 0;
                    row._locked = (row._open == 'o') ? 0 : 1;
                    
                    map[row._tid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.getPosts = function(callback) {
        return Exporter.getPaginatedPosts(0, -1, callback);
    };
    Exporter.getPaginatedPosts = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'exp_forum_posts.post_id as _pid, '
            + prefix + 'exp_forum_posts.topic_id as _tid, '
            + prefix + 'exp_forum_posts.author_id as _uid, '
//            + prefix + 'exp_members.username as _guest, '
            + prefix + 'exp_forum_posts.ip_address as _ip, '
            + prefix + 'exp_forum_posts.body as _content, '
            + prefix + 'exp_forum_posts.post_date as _timestamp '
            + 'FROM ' + prefix + 'exp_forum_posts WHERE ' + prefix + 'exp_forum_posts.topic_id<>0 '
//            + 'JOIN ' + prefix + 'exp_members ON ' + prefix + 'exp_forum_posts.author_id=' + prefix + 'exp_members.member_id '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._content = row._content || '';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    map[row._pid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.teardown = function(callback) {
        Exporter.log('teardown');
        Exporter.connection.end();

        Exporter.log('Done');
        callback();
    };

    Exporter.testrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getUsers(next);
            },
            function(next) {
                Exporter.getMessages(next);
            },
            function(next) {
                Exporter.getCategories(next);
            },
            function(next) {
                Exporter.getTopics(next);
            },
            function(next) {
                Exporter.getPosts(next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.paginatedTestrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getPaginatedUsers(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedMessages(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedCategories(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedTopics(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedPosts(1001, 2000, next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.warn = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.warn.apply(console, args);
    };

    Exporter.log = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.log.apply(console, args);
    };

    Exporter.error = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.error.apply(console, args);
    };

    Exporter.config = function(config, val) {
        if (config != null) {
            if (typeof config === 'object') {
                Exporter._config = config;
            } else if (typeof config === 'string') {
                if (val != null) {
                    Exporter._config = Exporter._config || {};
                    Exporter._config[config] = val;
                }
                return Exporter._config[config];
            }
        }
        return Exporter._config;
    };

    // from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
    Exporter.validateUrl = function(url) {
        var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
        return url && url.length < 2083 && url.match(pattern) ? url : '';
    };

    Exporter.truncateStr = function(str, len) {
        if (typeof str != 'string') return str;
        len = _.isNumber(len) && len > 3 ? len : 20;
        return str.length <= len ? str : str.substr(0, len - 3) + '...';
    };

    Exporter.whichIsFalsy = function(arr) {
        for (var i = 0; i < arr.length; i++) {
            if (!arr[i])
                return i;
        }
        return null;
    };

})(module.exports);
