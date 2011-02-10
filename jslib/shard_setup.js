// TODO:  Service options setup in layout - have both options and rtOptions along with the cl-specified options
// Allow disconnect/reconnect from sharded setup - put extra data in config database
// Look at ways of extracting PIDs from elsewhere

Service = function(host, port, connection, hostOptions) {
	this.port = port || 27017
	this.port = Number( this.port )
	this.host = host
	this.connection = connection || undefined
	this.hostOptions = hostOptions || {}
}

Service.prototype.getURL = function() {
	return ( this.host ? this.host.host : "" ) + ":" + this.port
}

Service.prototype.userDir = function() {
	return this.host.user ? this.host.user + "/" : "[default]/"
}

Service.prototype.toString = function() {
	return "service://" + this.getURL() + " @ " + ( this.host ? this.host.toString() : "" )
}

Service.hostFromName = function(name) {

	if ( name.indexOf( "ssh://" ) == 0 ) {
		host = SSHHost.fromName( name )
	}
	if ( name.indexOf( "local://" ) == 0 ) {
		host = LocalHost.fromName( name )
	}

	return host

}

Service.fromName = function(name, connection) {
	name = name.trim()

	var host = hostFromName( name )

	if ( !host )
		throw "Host type for '" + name + "' unknown."

	name = name.split( "//" )[1]
	var split = name.split( "/" )

	if ( split.length < 2 )
		throw "Port for service from '" + name + "' not found."

	return new Service( host, split[1], connection )

}

Service.prototype.start = function(options) {
	return this.host.start( this, options )
}

Service.prototype.restart = function(options) {
	if ( this.connection )
		this.stop()
	options = object.extendInPlace( { restart : true, resetDB : false }, options )
	return this.start( options )
}

Service.prototype.stop = function(options) {
	return this.host.stop( this, options )
}

_STATUS_ARBITER = 7

ShardedSetup = function(params) {

	var mongos = params.mongos
	var shards = params.shards
	var replicas = params.replicas
	var configs = params.configs

	this.uniquePorts = params.uniquePorts || false

	this.hosts = params.hosts || [ "local://default" ]
	if ( this.hosts.length == 0 )
		this.hosts = [ getHostName() ]

	this.layout = ShardedSetup.layoutFromName( params.layout || "default" )
	this.params = params

	for ( var i = 0; i < this.hosts.length; i++ )
		this.hosts[i] = Service.hostFromName( this.hosts[i] )

	var arrToObj = function(arr) {
		var obj = {}
		for ( var i = 0; i < arr.length; i++ ) {
			obj[i] = arr[i]
		}
		return obj
	}

	var asService = function(service, conn) {
		if ( typeof ( service ) != "object" )
			return Service.fromName( service, conn )
		return service
	}

	mongos = mongos || 1

	// Mongos
	// { <mongosName> : { <service> : <conn> },... }
	switch (typeof ( mongos )) {
	case "number":
	case "string":
		var numMongos = Number( mongos )
		mongos = arrToObj( new Array( numMongos ) )
		break;
	case "object":
		if ( mongos.length )
			mongos = arrToObj( mongos )
		for ( var key in mongos ) {
			// TODO: Choose services better
			mongos[key] = undefined
		}
		break;
	}

	var assocCount = function(obj) {
		var count = 0
		for ( i in obj )
			count++
		return count
	}

	this.mongos = mongos || {}
	this.numMongos = function() {
		return assocCount( this.mongos )
	}

	shards = shards || 1

	this.replicaDefaults = function() {
		return new Array( 3 )
	}

	// Shards
	// <number> | { <shardName=rsName> : <serviceDesc>,...}
	// <serviceDesc> ::= <number>|<rsNodeType>|[<rsNodeType>]
	// <rsNodeType> ::= undefined|<serviceType>
	// <serviceType> ::= $<string>|{<serviceString> : $type}
	// <serviceString> ::= <string>|<Service>
	// Goal is to get to [rsNodeType] representation after this, if possible
	// Otherwise can be undefined (i.e. replace if replicas not already there)
	switch (typeof ( shards )) {
	case "number":
	case "string":
		var numShards = Number( shards )
		shards = new Array( numShards )
	case "object":
		if ( shards.length )
			shards = arrToObj( shards )
		for ( var key in shards ) {
			switch (typeof ( shards[key] )) {
			case "number":
			case "string":
				var numReplicas = Number( shards[key] )
				shards[key] = new Array( numReplicas )
			case "object":
				if ( !shards[key].length )
					shards[key] = [ shards[key] ]
			}
		}
	}

	this.shards = shards
	this.numShards = function() {
		return assocCount( this.shards )
	}

	// Replicas
	// <number> | { <rsName=shardName> : [<rsNodeType>], ...}
	// Same as shards
	switch (typeof ( replicas )) {
	case "number":
	case "string":
		var numReplicasPerShard = Number( replicas )
		replicas = []
		for ( var name in shards )
			replicas.push( numReplicasPerShard )
	case "object":
		if ( replicas.length )
			replicas = arrToObj( replicas )

		for ( var key in replicas ) {
			if ( !replicas[key] ) {
				print( "Default replica set specified for shard '" + key + "'" )
				replicas[key] = this.replicaDefaults()
			}
			switch (typeof ( replicas[key] )) {
			case "number":
			case "string":
				var numReplicas = Number( replicas[key] )
				replicas[key] = new Array( numReplicas )
			case "object":
				if ( !replicas[key].length )
					replicas[key] = [ replicas[key] ]
			}
		}
	}

	this.replicas = replicas || {};
	this.numReplicas = function() {
		return assocCount( this.replicas )
	}

	// Create new replica sets for shards without replica sets
	for ( var shardName in this.shards ) {
		if ( shardName in this.replicas ) {
			this.shards[shardName] = this.replicas[shardName]
			continue
		}

		if ( !this.shards[shardName] ) {
			print( "Default replica set required for shard '" + shardName + "'" )
			this.shards[shardName] = this.replicas[shardName] = this.replicaDefaults()
		} else {
			this.replicas[shardName] = this.shards[shardName]
		}
	}

	// Create new shards for all the replica sets
	for ( var shardName in this.replicas ) {
		if ( !( shardName in this.shards ) )
			this.shards[shardName] = this.replicas[shardName]
	}

	configs = configs || 3

	// Configs
	// { <configName> : <server> }
	switch (typeof ( configs )) {
	case "number":
	case "string":
		var numConfigs = Number( configs )
		if ( numConfigs < 3 ) {
			print( "Cannot specify less than 3 config services." )
			numConfigs = 3
		}
		configs = arrToObj( new Array( numConfigs ) )
		break;
	case "object":
		if ( configs.length )
			configs = arrToObj( configs )
		for ( var key in configs ) {
			// TODO: Choose servers better
			configs[key] = undefined
		}
		break;
	}

	this.configs = configs || {}
	this.numConfigs = function() {
		return assocCount( this.configs )
	}

	this.services = {}
	this.numServices = function() {
		return assocCount( this.services )
	}

	this.usedPort = function(port) {
		for ( var url in this.services )
			if ( services[url].port == port )
				return true
		return false
	}

	this.asUniqueService = function(service, connection) {
		service = asService( service, connection )
		if ( !this.uniquePorts ) {
			while (service.getURL() in this.services) {
				service.port++
				service.connection = undefined
			}
		} else {
			while (this.usedPort( service.port )) {
				service.port++
				service.connection = undefined
			}
		}
		return service
	}

	// Fill in services for those undefined
	for ( var mongosName in this.mongos ) {
		if ( !this.mongos[mongosName] ) {
			this.mongos[mongosName] = new Service( undefined, undefined, undefined )
		} else {
			this.mongos[mongosName] = this.asService( this.mongos[mongosName] )
		}

		this.layout.placeMongosService( this, { name : mongosName, service : this.mongos[mongosName] } )

		print( "Placing mongos service @ " + this.mongos[mongosName].host )

		this.mongos[mongosName] = this.asUniqueService( this.mongos[mongosName] )
		this.services[this.mongos[mongosName].getURL()] = this.mongos[mongosName]
	}

	for ( var configName in this.configs ) {

		if ( !this.configs[configName] ) {
			this.configs[configName] = new Service( undefined, undefined, undefined )
		} else {
			this.configs[configName] = this.asService( this.configs[configName] )
		}

		this.layout.placeConfigService( this, { name : configName, service : this.configs[configName] } )

		print( "Placing config service @ " + this.configs[configName].host )

		this.configs[configName] = this.asUniqueService( this.configs[configName] )
		this.services[this.configs[configName].getURL()] = this.configs[configName]
	}

	// Convert all the replica types into services
	for ( var shardName in this.replicas ) {
		var types = this.replicas[shardName]

		// <rsNodeType> ::= undefined|<serverType>
		// <serverType> ::= <serverString>|$<typeString>|{$<typeString> :
		// <serverString>}|{type : $<typeString>,
		// server : <serverString>}
		// <serverString> ::= <string>|<Server>

		for ( var i = 0; i < types.length; i++ ) {

			var type = types[i]
			if ( !type )
				type = "$replica"

			switch (typeof ( type )) {
			case "string":
				var service = undefined
				if ( type.indexOf( '$' ) != 0 ) {
					service = type
					type = "$replica"
				}
				type = { type : type, service : service }
			case "object":
				// Pull out the first association by default, if can't find type
				if ( !type.type ) {
					for ( var firstType in type ) {
						type = { type : firstType, service : type[firstType] }
						break
					}
				}

				if ( !type.service ) {
					type.service = new Service( undefined, undefined, undefined )
				} else {
					type.service = this.asService( type.service )
				}

				this.layout.placeReplicaService( this, { shard : shardName, replica : i, service : type.service } )

				print( "Placing replica service @ " + type.service.host )

				type.service = this.asUniqueService( type.service )
				this.services[type.service.getURL()] = type.service
			}

			types[i] = type
		}
	}

}

ShardedSetup.layoutFromName = function(name) {
	var split = name.lastIndexOf( "Layout" )
	if ( split != name.length - "Layout".length )
		name += "Layout"

	return new ShardedSetup[name]()
}

ShardedSetup.controlFromName = function(name) {
	var split = name.lastIndexOf( "Control" )
	if ( split != name.length - "Control".length )
		name += "Control"

	return new ShardedSetup[name]()
}

ShardedSetup.defaultLayout = function() {

	this.count = 0;

	this.defaultConfigOptions = function(ss, context) {
		return {
			resetDB : true, dbpath : "$dbpath/config-" + context.name, port : context.service.port, noprealloc : "",
			smallfiles : "", oplogSize : "2", nohttpinterface : "" }
	}

	this.placeConfigService = function(ss, context) {
		if ( !context.service.host )
			context.service.host = ss.hosts[++this.count % ss.hosts.length]
		ss.asUniqueService( context.service )

		context.service.hostOptions = this.defaultConfigOptions( ss, context )
	}

	this.defaultReplicaOptions = function(ss, context) {
		return {
			resetDB : true, dbpath : "$dbpath/replica-" + context.shard + "-" + context.replica,
			port : context.service.port, oplogSize : "2", nohttpinterface : "" }
	}

	this.placeReplicaService = function(ss, context) {
		if ( !context.service.host )
			context.service.host = ss.hosts[++this.count % ss.hosts.length]
		ss.asUniqueService( context.service )

		context.service.hostOptions = this.defaultReplicaOptions( ss, context )
	}

	this.defaultMongosOptions = function(ss, context) {
		return { port : context.service.port, configdb : function(context) {
			return context.ss.configCluster.url
		} }
	}

	this.placeMongosService = function(ss, context) {
		if ( !context.service.host )
			context.service.host = ss.hosts[++this.count % ss.hosts.length]
		ss.asUniqueService( context.service )

		context.service.hostOptions = this.defaultMongosOptions( ss, context )
	}

}

ShardedSetup.prototype.init = function(name) {

	var dbRoot = this.params.dbRoot || "/data/"

	// Startup config instances
	var configOps = this.params.configOps || {}

	this.configCluster = []

	for ( var name in this.configs ) {
		var service = this.configs[name]

		var options = { serviceType : "mongod" }
		object.extendInPlace( options, service.hostOptions )
		object.extendInPlace( options, configOps )
		object.addContextInPlace( options, { ss : ss, service : service, name : name } )

		service.start( options )

		this["$config$" + name] = service.connection

		this.configCluster.push( service.getURL() )
	}

	var configClusterURL = this.configCluster.join( "," )
	this.configCluster = { url : configClusterURL, connection : new Mongo( configClusterURL ), getURL : function() {
		return configClusterURL
	} }
	this.$config = this.configCluster.connection

	var replicaOps = this.params.replicaOps || {}

	for ( var shardName in this.shards ) {

		var replicaSet = []
		var replicaSetConfig = []
		var numReplicas = this.replicas[shardName].length

		for ( var i = 0; i < this.replicas[shardName].length; i++ ) {
			var type = this.replicas[shardName][i]
			var service = type.service

			var options = { serviceType : "mongod", replSet : shardName, slaveOk : true }

			object.extendInPlace( options, service.hostOptions )
			object.extendInPlace( options, replicaOps )
			object.addContextInPlace( options, { ss : ss, service : service, shard : shardName, replica : i } )

			service.start( options )

			this["$replica$" + shardName + "$" + i] = service.connection

			replicaSet.push( service.getURL() )
			replicaSetConfig.push( { _id : replicaSetConfig.length, host : service.getURL() } )
		}

		replicaSetConfig = { _id : shardName, members : replicaSetConfig }

		print( "Initiating replica set '" + shardName + "'..." + tojson( replicaSetConfig ) )

		this.replicas[shardName][0].service.connection.getDB( "admin" ).runCommand(
				{ replSetInitiate : replicaSetConfig } )

		print( "Waiting for replica set '" + shardName + "' startup..." )

		this.waitForStartup( shardName )

		var shardURL = shardName + "/" + replicaSet.join( "," )

		this.shards[shardName] = {
			name : shardName, url : shardURL, connection : new Mongo( shardURL ), getURL : function() {
				return shardURL
			}, replicas : this.replicas[shardName] }
		this["$replica$" + shardName] = this.shards[shardName].connection
		this["$shard$" + shardName] = this.shards[shardName].connection

	}

	// Startup mongos instances
	var mongosOps = this.params.mongosOps || {}

	for ( var name in this.mongos ) {
		var service = this.mongos[name]

		var options = { serviceType : "mongos" }
		object.extendInPlace( options, service.hostOptions )
		object.extendInPlace( options, replicaOps )
		object.addContextInPlace( options, { ss : ss, service : service, name : name } )

		service.start( options )

		for ( var shardName in this.shards ) {
			service.connection.getDB( "admin" ).runCommand(
					{ addshard : this.shards[shardName].url, allowLocal : true } )
		}

		this["$mongos$" + name] = service.connection
	}

	this.$mongos = this.mongos[0].connection

	if ( this.mongos[0] ) {
		this.printShardingStatus()
		db = this.db = this.mongos[0].connection.getDB( "config" )
	}

}

ShardedSetup.prototype.printShardingStatus = function() {
	if ( !ss.mongos[0].connection )
		print( "Sharded setup not initialized." )
	else
		ss.mongos[0].connection.getDB( "admin" ).printShardingStatus()
}

ShardedSetup.prototype.end = function() {

	if ( db == this.db )
		db = undefined
	delete this.db

	for ( var name in this.mongos ) {
		var service = this.mongos[name]
		service.stop()

		delete this["$mongos$" + name]
	}

	delete this.$mongos

	for ( var shardName in this.shards ) {
		for ( var i = 0; i < this.replicas[shardName].length; i++ ) {

			var type = this.replicas[shardName][i]
			var service = type.service

			service.stop()

			delete this["$replica$" + shardName + "$" + i]
		}

		delete this.shards[shardName]
		delete this["$replica$" + shardName]
		delete this["$shard$" + shardName]
	}

	for ( var name in this.configs ) {
		var service = this.configs[name]

		service.stop()

		delete this["$config$" + name]
	}

	delete this.configCluster
	delete this.$config

}

ShardedSetup.prototype.waitForStartup = function(shardName) {

	var replicas = this.replicas

	ShardedSetup.waitFor( function() {

		for ( var i in replicas[shardName] ) {
			var connection = replicas[shardName][i].service.connection
			if ( connection ) {
				var status = connection.getDB( "admin" ).runCommand( { replSetGetStatus : 1 } )

				// Continue if connected to an arbiter
				if ( status.startupStatus == _STATUS_ARBITER )
					continue

				if ( !status.ok )
					return undefined

				for ( var j = 0; j < status.members.length; j++ ) {
					if ( !( status.members[j].stateStr == "PRIMARY" || status.members[j].stateStr == "SECONDARY" ) )
						return undefined
				}
			}
		}

		return true

	} )

}

ShardedSetup.waitForMongo = function(url, initSleep) {

	print( "Waiting for mongo daemon at " + url )
	// Need to heuristically wait here, just in case an instance takes a long
	// time to go down.
	sleep( initSleep || 1000 )
	return ShardedSetup.waitFor( 300, function() {
		try {
			mongo = new Mongo( url )
			return mongo
		} catch (e) {
			return undefined
		}
	} )

}

ShardedSetup.waitForNoMongo = function(url, initSleep) {

	print( "Waiting for mongo daemon to shut down at " + url )
	// Need to heuristically wait here, so we con't connect to an instance going
	// down (fix available)
	sleep( initSleep || 1000 )
	return ShardedSetup.waitFor( 300, function() {
		try {
			new Mongo( url )
			return undefined
		} catch (e) {
			return true
		}
	} )

}

ShardedSetup.waitFor = function(time, limit, f) {

	if ( typeof ( time ) == "function" ) {
		f = time
		time = limit = undefined
	} else if ( typeof ( limit ) == "function" ) {
		f = limit
		limit = undefined
	}

	time = time || 500

	var i = 0
	var result = undefined
	do {
		if ( ++i == limit ) {
			break
		}

		sleep( time )

		try {
			result = f()
		} catch (e) {
			print( e )
		}
	} while (result == undefined)

	return result

}

ShardedSetup.copyOptions = function(options) {
	var newOptions = {}
	for ( option in options ) {
		newOptions[option] = options[option]
	}
	return newOptions
}

ShardedSetup.asCommand = function(commands, options, prefix) {

	if ( typeof ( commands ) != "object" )
		commands = [ commands ]
	if ( !commands.length )
		commands = [ commands ]

	prefix = prefix || ""

	for ( option in options ) {
		commands.push( prefix + option )
		if ( options[option] != null && options[option] != undefined )
			commands.push( options[option] )
	}

	return commands.join( " " )

}

ShardedSetup.prototype.getSecondaryReplicas = function(shard) {

	var shardName = shard
	if ( typeof ( shard ) == "object" )
		shardName = shard.name

	var replicas = this.replicas[shardName]
	var secondaries = []

	for ( var i = 0; i < replicas.length; i++ ) {
		if ( replicas[i].service.connection ) {
			var result = replicas[i].service.connection.getDB( "admin" ).runCommand( { ismaster : 1 } )
			if ( result && result.secondary )
				secondaries.push( replicas[i] )
		}
	}

	return secondaries

}

ShardedSetup.prototype.getMasterReplica = function(shard) {

	var shardName = shard
	if ( typeof ( shard ) == "object" )
		shardName = shard.name

	var replicas = this.replicas[shardName]

	for ( var i = 0; i < replicas.length; i++ ) {
		if ( replicas[i].service.connection ) {
			var result = replicas[i].service.connection.getDB( "admin" ).runCommand( { ismaster : 1 } )
			if ( result && result.ismaster )
				return replicas[i]
			else if ( !result )
				print( "Could not get ismaster result for shard '" + shardName + "', service "
						+ replicas[i].service.connection )
		}
	}

	return undefined

}

ShardedSetup.prototype.$active = function() {
	var active = this.activeMongos()
	if ( active )
		return active.connection
}

ShardedSetup.prototype.activeMongos = function() {
	for ( var name in this.mongos )
		if ( this.mongos[name].connection )
			return this.mongos[name]
}

ShardedSetup.prototype.getDBShard = function(db) {
	return this.getDBInfo( db ).primary
}

ShardedSetup.prototype.getDBInfo = function(db) {

	var connection = this.$config
	if ( !connection ) {
		print( "Config connection is not active, cannot get DB info." )
		return undefined
	}

	if ( db ) {
		return connection.getDB( "config" ).databases.findOne( { _id : db.toString() } )
	} else {
		return connection.getDB( "config" ).databases.find()
	}

}

ShardedSetup.localFile = function() {
	var error = new Error();
	return error.fileName
}()

ShardedSetup.localDir = function() {
	var filename = ShardedSetup.localFile

	var split = filename.lastIndexOf( "/" )
	if ( split < 0 )
		split = filename.lastIndexOf( "\\" )
	if ( split < 0 )
		return "./"
	else
		return filename.substring( 0, split ) + "/"
}()

object = new function() {
}

object.extendInPlace = function(obj, ext) {

	ext = ext || {}

	for ( var key in ext ) {
		obj[key] = ext[key]
	}

	return obj
}

object.addContextInPlace = function(obj, context) {
	for ( var key in obj ) {
		var value = obj[key]
		if ( typeof ( value ) == "function" )
			value = value( context )
		obj[key] = value
	}

	return obj
}

object.toJSON = function(type, json) {
	json.__class__ = type
	return json
}

object.fromJSON = function(json) {
	return eval( json.__class__ + ".fromJSON( json )" )
}

load( ShardedSetup.localDir + 'shard_setup_ssh.js' )
load( ShardedSetup.localDir + 'shard_setup_local.js' )
