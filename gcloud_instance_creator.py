"""Script to create a new compute instance."""
import datetime
import argparse
import json
import logging
import sys
import subprocess

DISK_TYPE = 'standard' #ssd
BOOT_DISK_SIZE = '4096GB'
INSTANCE_NAME = 'cnc-compute-server'
ZONE = 'us-west1-b'
SNAPSHOT_NAME = '%s-snapshot'
N_CPUS_DEFAULT = 1


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():

    parser = argparse.ArgumentParser(description='Maintain compute instance.')
    parser.add_argument('--delete', action='store_true')
    parser.add_argument('--create', action='store_true')
    parser.add_argument('--get-ip', action='store_true')
    parser.add_argument('--archive', action='store_true')
    parser.add_argument('--restore', action='store_true')
    parser.add_argument('--ncpus', action='store', default=N_CPUS_DEFAULT)
    parser.add_argument('--disksize', action='store', default=BOOT_DISK_SIZE)
    parser.add_argument('--name', action='store', default=INSTANCE_NAME)
    parser.add_argument('--ramgb', action='store', default=N_CPUS_DEFAULT)
    parser.add_argument('--stop', action='store_true')
    parser.add_argument(
        '--change-cpu-ram', action='store_true')

    args = parser.parse_args()
    snapshot_name = '%s-snapshot' % args.name
    ram = 1024 * int(args.ramgb)
    if args.create:
        LOGGER.info("creating %s", args.name)
        compute_creation_cmd = """gcloud compute --project=ecoshard-202922 instances create %s --zone=%s --machine-type=custom-%s-%s --subnet=default --network-tier=PREMIUM --metadata=startup-script="sudo apt update;sudo apt upgrade -y;sudo apt install -y mercurial;sudo apt install -y python3-gdal python-gdal;sudo apt install -y emacs;sudo apt install -y python-pip python3-pip;sudo apt install -y cython3 cython;sudo apt install -y python-rtree python3-rtree" --maintenance-policy=MIGRATE --service-account=587088520267-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append --image=ubuntu-minimal-1904-disco-v20190417 --image-project=ubuntu-os-cloud  --boot-disk-size=%s --boot-disk-type=pd-%s""" % (
            args.name, ZONE, args.ncpus, ram, args.disksize,
            DISK_TYPE)
        LOGGER.info('creating with command: %s', compute_creation_cmd)
        result_str = (
            subprocess.check_output(compute_creation_cmd, shell=True))
        LOGGER.info("result: %s", result_str)

    if args.change_cpu_ram:
        LOGGER.info(
            "setting %s cpu/ram to: %s %s", args.name, args.ncpus, ram)
        stop_instance(args.name)
        LOGGER.info('stopping %s', args.name)
        set_machine_type_cmd = (
            'gcloud compute instances set-machine-type %s '
            '--machine-type=custom-%s-%s' % (args.name, args.ncpus, ram))
        result_str = (
            subprocess.check_output(set_machine_type_cmd, shell=True))
        start_instance(args.name)
        LOGGER.info('new cpu and ram!')

    if args.get_ip:
        get_ip_cmd = """gcloud compute instances describe %s --zone=%s --format=json""" % (args.name, ZONE)
        LOGGER.info("querying google with: %s", get_ip_cmd)
        result_json = json.loads(
            subprocess.check_output(get_ip_cmd, shell=True))
        LOGGER.info(result_json['networkInterfaces'][0]['accessConfigs'][0]['natIP'])

    if args.delete:
        delete_instance(args.name)

    if args.stop:
        stop_instance(args.name)

    if args.archive:
        LOGGER.info("archiving %s", args.name)
        stop_instance(args.name)
        LOGGER.info('snapshotting %s', args.name)
        snapshot_description = 'archived via script on %s' % (
            datetime.datetime.now().isoformat())
        snapshot_cmd = 'gcloud compute disks snapshot %s --description="%s" --snapshot-names=%s --zone=%s' % (
            args.name, snapshot_description, snapshot_name, ZONE)
        result_str = (subprocess.check_output(snapshot_cmd, shell=True))
        LOGGER.info("result: %s", result_str)
        delete_instance(args.name, bonus_flags='-q')

    if args.restore:
        LOGGER.info('restore %s', args.name)
        new_disk_cmd = """gcloud compute disks create %s --size %s --zone %s --source-snapshot %s --type pd-%s """ % (args.name, args.disksize, ZONE, snapshot_name, DISK_TYPE)
        LOGGER.info('creating boot disk: %s', new_disk_cmd)
        result_str = (subprocess.check_output(new_disk_cmd, shell=True))
        LOGGER.info("result: %s", result_str)
        compute_creation_cmd = """gcloud compute --project=ecoshard-202922 instances create %s --zone=%s --machine-type=custom-%s-%s --subnet=default --network-tier=PREMIUM --maintenance-policy=MIGRATE --service-account=587088520267-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append --disk=name=%s,device-name=%s,mode=rw,boot=yes,auto-delete=yes""" % (
            args.name, ZONE, args.ncpus, ram, args.name,
            args.name)
        LOGGER.info('creating with command: %s', compute_creation_cmd)
        result_str = (
            subprocess.check_output(compute_creation_cmd, shell=True))
        LOGGER.info("result: %s", result_str)

        snapshot_delete_cmd = (
            "gcloud compute snapshots delete %s -q" % snapshot_name)
        LOGGER.info("deleting snapshot with: %s", snapshot_delete_cmd)
        result_str = (
            subprocess.check_output(snapshot_delete_cmd, shell=True))
        LOGGER.info("result: %s", result_str)


def stop_instance(instance_name):
    """Stop the instance."""
    stop_cmd = """gcloud compute instances stop %s --zone=%s""" % (
        instance_name, ZONE)
    LOGGER.info('stopping with command %s', stop_cmd)
    result_str = subprocess.check_output(stop_cmd, shell=True)
    LOGGER.info('stopped: %s', result_str)


def start_instance(instance_name):
    """Start the instance."""
    start_cmd = """gcloud compute instances start %s --zone=%s""" % (
        instance_name, ZONE)
    LOGGER.info('started with command %s', start_cmd)
    result_str = subprocess.check_output(start_cmd, shell=True)
    LOGGER.info('started: %s', result_str)


def delete_instance(instance_name, bonus_flags=''):
    """Delete the instance."""
    LOGGER.info("deleting %s", instance_name)
    delete_cmd = (
        "gcloud compute instances delete %s --zone=%s " + bonus_flags) % (
            instance_name, ZONE)
    result_str = subprocess.check_output(delete_cmd, shell=True)
    LOGGER.info("result: %s", result_str)


if __name__ == '__main__':
    main()
