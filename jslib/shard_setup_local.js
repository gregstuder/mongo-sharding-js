// LocalHost host
// Runs mongo services on the localhost
// 
// General contract:
//  toString()
//  fromName()
//  start( options ) starts a service on the (local)host
//  stop( options ) stops a service on the (local)host
//
//  options are a mongo-storable object with parameters for running the service
//  contain a serviceType, which tells which mongo service to run

LocalHost = function(name) {
	this.name = name
	this.host = getHostName()
}

LocalHost.prototype.toString = function() {
	return "local://" + this.name
}

LocalHost.prototype.toJSON = function() {
	return object.toJSON( "LocalHost", { name : this.name } )
}

LocalHost.fromJSON = function(json) {
	return new LocalHost( json.name )
}

LocalHost.fromName = function(name) {

	name = name.trim()

	if ( name.indexOf( "local://" ) == 0 )
		name = name.substring( "local://".length )

	var split = name.indexOf( "/" )
	if ( split >= 0 )
		name = name.substring( 0, split )

	return new LocalHost( name )

}

LocalHost.prototype.start = function(service, options) {

	// If we're restarting this service, we use the previous runtime options
	if ( options.restart ) {
		delete options.restart
		options = object.extendInPlace( service.rtOptions, options )
	}
	delete service.rtOptions

	var rtOptions = ShardedSetup.copyOptions( options )

	var serviceType = options.serviceType
	delete options.serviceType

	var rv = false

	if ( serviceType == "mongod" )
		rv = this.startMongod( service, options )
	else if ( serviceType == "mongos" )
		rv = this.startMongos( service, options )

	if ( rv ) {
		service.rtOptions = rtOptions

		// Wait until we have all the data for the process in the DB
		ShardedSetup.waitFor( function() {
			return rv.process.count( { _id : service.getURL } ) > 0
		} )

		// Update the process with the runtime info
		rv.process.update( { _id : service.getURL() },
				{ $set : { host : this.toJSON(), rtOptions : service.rtOptions } } );
		return rv.process.find( { _id : service.rtOptions } )
	}

	return false
}

LocalHost.prototype.startMongod = function(service, options) {

	var defaultDBPath = "/data/_local_"
	options.dbpath = options.dbpath.replace( "$dbpath", defaultDBPath )

	options.fork = null
	var defaultLogPath = options.dbpath + "/mongod"
	options.logpath = options.logpath || "$logpath.log"
	options.logpath = options.logpath.replace( "$logpath", defaultLogPath )
	var logPath = options.logpath

	var resetDB = options.resetDB
	delete options.resetDB

	var slaveOk = options.slaveOk
	delete options.slaveOk

	var command = "mkdir -p " + options.dbpath + " ; " + ShardedSetup.asCommand( "mongod", options, "--" )

	// For safety, make sure we only ever wipe out stuff in the /data directory
	if ( resetDB && options.dbpath.length > 0 && options.dbpath.indexOf( "/data" ) == 0 )
		command = "rm -R " + options.dbpath + "/* ; " + command

	this.bashRun( service, command )

	var mongo = service.connection = ShardedSetup.waitForMongo( service.getURL() )
	if ( slaveOk )
		mongo.setSlaveOk()

	command = "mongo localhost:" + service.port + "/local --eval \"" + "db.process.save({ _id : '" + service.getURL()
			+ "', pid : cat('" + logPath + "').split('pid=')[1].split(/\\s/)[0] }, { upsert : true })" + "\""

	this.bashRun( service, command )

	return mongo.getDB( "local" )

}

LocalHost.prototype.startMongos = function(service, options) {

	options.fork = null
	var defaultLogPath = "/data/_local_/mongos." + service.host.host + "." + service.port
	options.logpath = options.logpath || "$logpath.log"
	options.logpath = options.logpath.replace( "$logpath", defaultLogPath )

	var command = ShardedSetup.asCommand( "mongos", options, "--" )
	var startLogPath = options.logpath + ".start"
	command += " > " + startLogPath

	this.bashRun( service, command )

	var mongo = service.connection = ShardedSetup.waitForMongo( service.getURL() )

	command = "mongo localhost:" + service.port + "/config --eval \"" + "db.process.save({ _id : '" + service.getURL()
			+ "', pid : cat('" + startLogPath + "').split('process: ')[1].split(/\\s/)[0] }, { upsert : true })" + "\""

	this.bashRun( service, command )

	return mongo.getDB( "config" )
}

LocalHost.prototype.stop = function(service, options) {

	options = options || {}

	if ( !service.connection )
		return false

	var pidDB = undefined
	if ( service.rtOptions.serviceType == "mongod" )
		pidDB = service.connection.getDB( "local" )
	else if ( service.rtOptions.serviceType == "mongos" )
		pidDB = service.connection.getDB( "config" )

	var pid = pidDB.process.findAndModify( { query : { _id : service.getURL() }, remove : true } ).pid

	var command = "kill " + pid

	this.bashRun( service, command )

	ShardedSetup.waitForNoMongo( service.getURL() )

	service.connection = undefined

	return true
}

LocalHost.prototype.bashRun = function(service, command) {
	var bashCommand = [ "bash", "-c" ]
	bashCommand.push( command )
	return runProgram.apply( null, bashCommand )
}
