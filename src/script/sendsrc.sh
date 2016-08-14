#!bin/bash
echo "============= Kill ============="
pssh -h ~/server.txt -l gfs -t 3600 -A -i "pkill stress_node"
echo "============= Remove ============="
pssh -h ~/all.txt -l gfs -A -i rm -rf /home/gfs/zlm
echo "============= Send ============="
pscp -r -h ~/all.txt -l gfs -A ~/project/ppca-gfs/ /home/gfs/zlm

