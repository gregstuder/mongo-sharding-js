// Tests shutdown of shards 

// For now, we have to be in the root directory of the project, could fix 
// but would have to touch the shell code
load( "jslib/shard_setup.js" )

var ss = new ShardedSetup( { mongos : 1, shards : [ 1, 2 ] } )

ss.init()

// Generate a bunch of random data
_srand( 1 )

var numDBs = [ 2, 6 ]
var numCollections = [ 3, 5 ]
var numElements = [ 1000, 3000 ]
var numEvents = [ 3, 5 ]

var rrange = function(range) {

	var from = range[0]
	if ( "from" in range )
		from = range.from
	var to = range[1]
	if ( "to" in range )
		to = range.to

	if ( from > to ) {
		var swap = to
		to = from
		from = swap
	}

	var spread = to - from;
	return Math.floor( _rand() * spread + from )
}

numDBs = rrange( numDBs )
numCollections = rrange( numCollections )
numElements = rrange( numElements )

for ( var i = 0; i < numDBs; i++ ) {

	var db = ss.$active().getDB( "myDB_" + i )

	for ( var j = 0; j < numCollections; j++ ) {

		var collection = db.getCollection( "myColl_" + j )

		for ( var k = 0; k < numElements; k++ ) {

			collection.save( { count : k, db : db.toString(), collection : collection.toString(), data : _rand() } )

		}
	}
}

// TODO: Make this better
print( "Waiting for replication to happen." )
sleep( 10000 )

var randDB = function() {

	print( "Getting DB info..." )
	var dbInfo = ss.getDBInfo().toArray()
	var dbNum = rrange( [ 0, dbInfo.length ] )

	print( "Getting random DB from info..." )
	var dbCount = 0
	var db = undefined
	while (( !db || ( db.toString() in { "config" : 1, "admin" : 1, "local" : 1 } ) ) && dbCount < dbInfo.length) {
		db = ss.$active().getDB( dbInfo[( dbNum + dbCount ) % dbInfo.length]._id )
		dbCount++
	}

	return db

}

var randCollection = function(db) {

	print( "Getting collection names..." )
	db = db || randDB()
	var collNames = db.getCollectionNames()

	var collNum = rrange( [ 0, collNames.length ] )
	var collCount = 0
	var collName = undefined
	while (( !collName || collName.indexOf( "system" ) == 0 ) && collCount < collNames.length) {
		collName = collNames[( collNum + collCount ) % collNames.length]
		collCount++
	}

	print( "Returing collection from DB..." )
	if ( collName )
		return db.getCollection( collName )
	else
		return undefined

}

ss.printShardingStatus()

numEvents = rrange( numEvents )
numEvents = 1000

for ( var i = 0; i < numEvents; i++ ) {

	if ( true ) {

		print( "\n\n\nEvent " + i )

		print( "Finding random DB on active mongos..." )
		var db = randDB()
		print( "Random DB found." )

		var dbName = db.toString()

		print( "Finding DB shard and replicas..." )
		var shard = ss.getDBShard( db )
		var masterService = ss.getMasterReplica( shard ).service
		var numSecondaries = ss.getSecondaryReplicas( shard ).length

		print( "Finding random collection on active mongos..." )
		var collection = randCollection( db )
		print( "Random collection found." )

		var exception = undefined
		var result = undefined

		// Stop the service, which may or may not be slaveOk'd / replicated
		print( "Stopping master service at " + masterService.getURL() )
		masterService.stop()

		var slaveOk = _rand() > 0.5

		if ( slaveOk )
			collection._mongo.slaveOk = true
		else
			collection._mongo.slaveOk = undefined

		print( "Master service stopped..." )
		try {
			result = collection.findOne()
		} catch (e) {
			exception = e
		}

		// Check for error if not slaveOk'd / replicated, otherwise good result
		if ( result && result.data && ( numSecondaries > 0 && slaveOk ) )
			print( "Correctly found result data " + tojson( result ) + " from secondaries." )
		else if ( exception && ( slaveOk == false || numSecondaries == 0 ) ) {
			print( "Correctly failed to find result data from non-replicated set." )
		} else {
			assert( false, "Got result with slaveOk : " + slaveOk + ", " + numSecondaries + " secondar(ies): "
					+ tojson( result ? result : undefined ) + " / " + exception )
		}

		print( "Restarting mongo service at " + masterService.getURL() )
		masterService.restart()

		// Wait until our shard has a master replica again
		var masterReplica = undefined
		print( "Waiting for master to reinitialize." )
		while (!masterReplica)
			masterReplica = ss.getMasterReplica( shard )

	}

}
