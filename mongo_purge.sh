#!/bin/bash
# This script deletes data older than the date provided as argument
# DBOPS-8525
# Author = Vamsi Krishna

# Functions
usage() {

[[ ! -z $1 ]] && echo $1

cat << EOF
	Required:
	=============
		- u : Username to connect to the mongodb instance
		- p : Password for the provided username
		- a : authentication database to use for mongodb log in 
		- h : hostname this script should run against (Defaults to localhost)
		- d : Database name to use for purging.
		- c : Colleciton to run purge against.
		- o : ObjectId of max date to delete older data than selected date.
			You can get the ObjectId for a date here --< https://steveridout.github.io/mongo-object-time/ >-- 
		- t : date time of max date to delete older than provided date.
			please chose date time wisely according to the time_zone of the data in the collection.

	Note:
	============
		-- Either one ObjectId or date time is only required. Script throws an error if both are supplied or none.
		-- date time format must be like "yyyy-mm-dd hh:mm:ss"

	Examples:
	===========
		-- Delete data using ObjectID
			./$(basename $0) -d test -c thirdPartyTracking -o "58966b000000000000000000"  
		-- Delete data using date time
			./$(basename $0) -d test -c thirdPartyTracking -t "2017-01-01 00:00:00"
EOF

exit 1
}

log() {
        echo "[`date +'%Y-%m-%d %H:%M:%S'`]: $1"
}

exit_check() {
	EXIT_VAL=$?
	if [[ $EXIT_VAL -ne 0 ]];then
		log "$1" 
		exit 1
	fi
}

validate_dateTime() {
	Time=`echo $dateTime | cut -f2 -d' '`
	Date=`echo $dateTime | cut -f1 -d' '`

	if [[ $Time =~ ^([0-9]{2}):([0-9]{2}):([0-9]{2})$ ]]; then  # Validating time string
        	if (( BASH_REMATCH[3] > 59 )) || (( BASH_REMATCH[2] > 59 )) || (( BASH_REMATCH[1] > 23 )); then
        	        time_valid=1	#echo "Wrong time format"
        	else
        	        time_valid=0	#echo "Time format is valid"
        	fi
	fi

	date "+%Y-%m-%d" -d "$Date" > /dev/null 2>&1 # Validating date string
	is_valid=$?
	if [[ $is_valid -eq 0 ]];then
		date_valid=0	#echo "date format is valid"
	elif [[ $is_valid -ne 0 ]];then
		date_valid=1	#echo "date format is not valid"
	fi

	if [[ $time_valid -eq 0 ]] && [[ $date_valid -eq 0 ]];then
		log "Date time format provided is valid."
	elif [[ $time_valid -ne 0 ]] || [[ $date_valid -ne 0 ]];then
		log "Wrong date time format provided. Exiting..." && exit 1
	fi
}

validate_objectid() {
	log "Validating ObjectId ['$1']"
	evalu=`$MON_CONN --eval "ObjectId('${1}').getTimestamp();" | awk 'NR>2'`
	exit_check "Unable to connect to mongod. Exiting..."
	if [[ `echo $evalu | grep -ic error` -gt 0 ]];then
		log "Invalid ObjectId. This is usually a 24 character hex string"
		usage "Actual error message: $evalu"
	else
		log "ObjectId ["$1"] is Valid."
	fi
}

obtain_objectid() {
	mongoQuery=`echo $1 | sed -e 's/'\ '/T/g' -e 's/$/Z/g' | sed -e 's/^/ObjectId.fromDate\(ISODate\(\"/g' -e 's/$/\"\)\)/g'`
	oId=`$MON_CONN --eval "$mongoQuery" | awk 'NR==3'`
	exit_check "Unable to connect to mongod. Exiting..."
	objectid=`echo $oId | cut -f2 -d'"'`
	evalu=$dateTime	
}

create_jsfile() {
echo -e "
// Database to use
use ${1};

// Finding IDs older than given timestamp ObjectId
// ObjectIds that match the criteria are stored in the array 'removeIdsArray'
// Ids are removed in a loop to avoid running the query forever and run to never completion

var cnt=db.${2}.find({_id: {\$lt:ObjectId(\"${3}\")}}, {_id:1}).count();
print('[', Date(), ']', ':', 'Total Documents to delete', '-', cnt);

while (cnt>0) {
	var removeIdsArray=db.${2}.find({_id: {\$lt:ObjectId(\"${3}\")}}, {_id:1}).limit(3000).toArray().map(function(doc) {return doc._id;});
	removeIdsArray.forEach( function(OID) { db.${2}.remove(OID)});
	var cnt=cnt-3000;
	if (cnt>0) {print('[', Date(), ']', ':', 'Documents Remaining', '-', cnt);} else {print('[', Date(), ']', ':','Deletion is complete');}
};"

}

run_js() {
	log "Removing documents older than [$evalu]."
	$MON_CONN < $jsfile &
	exit_check "Failed at running javaScript file. Exiting..."
}

# Variables
jsfile=purge_`date +%Y%m%d%H%M%S`.js

while getopts "u:p:h:a:d:c:o:t:" option;do

	case "$option" in
		u) user=${OPTARG};;
		p) pass=${OPTARG};;
		h) host=${OPTARG};;
		a) auth=${OPTARG};;
		d) database=${OPTARG};;
		c) collection=${OPTARG};;
		o) objectid=${OPTARG};;
		t) dateTime=${OPTARG};;
		*) usage;;
	esac
	allargs+=$OPTARG
done

# Validations
host=${host:-localhost}

if [[ -z $database ]] || [[ -z $collection ]];then
	usage "ERROR: One or more options (database/colleciton) are missing"
fi

if [[ -z $objectid ]] && [[ -z $dateTime ]];then
	usage "One or more options (objectid/dateTime) are missing"
elif [[ ! -z $dateTime ]] && [[ ! -z $objectid ]];then
	usage "Both time and objectid are provided. Only one of these are essential"
fi

if [[ -z $user ]] || [[ -z $pass ]] || [[ -z $auth ]];then
        usage "ERROR: One or more options (username/password/authenticationDatabase) for connecting to mongo instance are missing."
else
        MON_CONN="mongo -u$user -p$pass --host $host --authenticationDatabase $auth"
fi

# Main Code
[[ ! -z $objectid ]] && validate_objectid "$objectid"
[[ ! -z $dateTime ]] && validate_dateTime
[[ ! -z $dateTime ]] && obtain_objectid "$dateTime"

log "Creating javascript file [$jsfile] on the fly..."
create_jsfile "$database" "$collection" "$objectid" > $jsfile
run_js 
wait 
if [[ $? -eq 0 ]];then
	log "{ Run : "Success", Exit : 0 }"
else
	log "{ Run : "Failed", Exit : 0, msg:"Please check the error and take correct action" }"
fi
exit 0


