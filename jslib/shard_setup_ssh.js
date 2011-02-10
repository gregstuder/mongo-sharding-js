// Sharding with SSH

SSHHost = function(user, password, host, port) {
	this.user = user || undefined
	this.password = password || undefined
	this.host = host
	this.port = port || 22
	this.port = Number( this.port )
}

SSHHost.prototype.toString = function() {
	var str = "ssh://"

	if ( !this.user )
		str += "<default>"
	else
		str += this.user
	if ( !this.password )
		str += ":<?>"
	else
		str += ":<password>"

	str += "@" + this.host

	str += ":" + this.port

	return str
}

SSHHost.fromName = function(name) {

	name = name.trim()

	if ( name.indexOf( "ssh://" ) == 0 )
		name = name.substring( "ssh://".length )

	var split = name.indexOf( "/" )
	if ( split >= 0 )
		name = name.substring( 0, split )

	var split = name.split( "@" )
	var host = split[0]
	var user = null
	if ( split.length > 1 ) {
		host = split[1]
		user = split[0]
	}

	split = host.indexOf( ":" );
	split = split < 0 ? host.length : split
	var port = host.substring( split + 1 )
	host = host.substring( 0, split )

	if ( user ) {
		split = user.indexOf( ":" )
		split = split < 0 ? user.length : split
		var password = user.substring( split + 1 )
		user = user.substring( 0, split )
	} else {
		user = password = undefined
	}

	print( "Generating ssh host from " + name + " to " + new SSHHost( user, password, host, port ).toString() )

	return new SSHHost( user, password, host, port )
}

SSHHost.prototype.start = function(service, options) {

	var rtOptions = ShardedSetup.copyOptions( options )

	var serviceType = options.serviceType
	delete options.serviceType

	object.extendInPlace( options, hostOptions )

	var rv = false

	if ( serviceType == "mongod" )
		rv = this.startMongod( service, options )
	else if ( serviceType == "mongos" )
		rv = this.startMongos( service, options )

	if ( rv )
		service.rtOptions = rtOptions
	else
		delete service.rtOptions

	return rv
}

SSHHost.prototype.startMongod = function(service, options) {

	var defaultDBPath = "/data/" + ( this.user ? this.user : "_default_" )
	options.dbpath = options.dbpath.replace( "$dbpath", defaultDBPath )

	options.fork = null
	var defaultLogPath = options.dbpath + "/mongod"
	options.logpath = options.logpath || "$logpath.log"
	options.logpath = options.logpath.replace( "$logpath", defaultLogPath )
	var logPath = options.logpath

	var resetDB = options.resetDB
	delete options.resetDB

	var command = "mkdir -p " + options.dbpath + " ; " + ShardedSetup.asCommand( "mongod", options, "--" )

	// For safety, make sure we only ever wipe out stuff in the /data directory
	if ( resetDB && options.dbpath.length > 0 && options.dbpath.indexOf( "/data" ) == 0 )
		command = "rm -R " + options.dbpath + "/* ; " + command

	this.sshRun( service, command )

	var mongo = service.connection = ShardedSetup.waitForMongo( service.getURL() )

	command = "mongo localhost:" + service.port + "/local --eval \"" + "db.process.save({ _id : '" + service.getURL()
			+ "', pid : cat('" + logPath + "').split('pid=')[1].split(/\\s/)[0] }, { upsert : true })" + "\""

	this.sshRun( service, command )

	return true

}

SSHHost.prototype.startMongos = function(service, options) {

	options.fork = null
	var defaultLogPath = "/data/" + ( this.user ? this.user : "_default_" ) + "/mongos." + service.host.host + "."
			+ service.port
	options.logpath = options.logpath || "$logpath.log"
	options.logpath = options.logpath.replace( "$logpath", defaultLogPath )

	var command = ShardedSetup.asCommand( "mongos", options, "--" )
	var startLogPath = options.logpath + ".start"
	command += " > " + startLogPath

	this.sshRun( service, command )

	var mongo = service.connection = ShardedSetup.waitForMongo( service.getURL() )

	command = "mongo localhost:" + service.port + "/config --eval \"" + "db.process.save({ _id : '" + service.getURL()
			+ "', pid : cat('" + startLogPath + "').split('process: ')[1].split(/\\s/)[0] }, { upsert : true })" + "\""

	this.sshRun( service, command )

	return mongo
}

SSHHost.prototype.stop = function(service, options) {

	options = options || {}

	if ( !service.rtOptions )
		return false

	var pidDB = undefined
	if ( service.options.serviceType == "mongod" )
		pidDB = service.connection.getDB( "local" )
	else if ( service.options.serviceType == "mongos" )
		pidDB = service.connection.getDB( "config" )

	var pid = pidDB.process.findOne( { _id : service.getURL() } ).pid

	var command = "kill " + pid

	this.sshRun( service, command )

	ShardedSetup.waitForNoMongo( service.getURL() )

	service.connection = undefined
	delete service.rtOptions

	return true
}

SSHHost.prototype.sshRun = function(service, command) {
	var ssh = service.host
	var sshCommand = [ "ssh", ssh.host, "-p", ssh.port ]
	if ( ssh.user )
		sshCommand = sshCommand.concat( [ "-l", ssh.user ] )
	sshCommand = sshCommand.concat( command )

	return runProgram.apply( null, sshCommand )
}