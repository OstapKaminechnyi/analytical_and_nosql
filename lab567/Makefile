set_project:
	gcloud config set core/project lyrical-amulet-276713 
create_instance:
	gcloud redis instances create myinstance --size=2 --region=us-central1 --redis-version=redis_4_0
instance_describe:
	gcloud redis instances describe myinstance --region=us-central1

HOST=10.200.38.227
PORT=6379

install_telnet:
	sudo apt-get install telnet -y
connect_to_redis:
	telnet $(HOST) $(PORT) 

set_zone:
	gcloud config set compute/zone us-central1-a



export_vars:
	export GCS_BUCKET_NAME=redis-iot/


deploy_new_instance:
	gcloud compute instances create example-instance \
    --metadata-from-file startup-script=startup-script.sh