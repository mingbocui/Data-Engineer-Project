scp -i ***.pem etl.py dl.cfg hadoop@ec2-***compute-1.amazonaws.com:~/

ssh -i ~/mykeypair.pem hadoop@ec2-3-226-76-70.compute-1.amazonaws.com

pip install awscli
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install openssh-client

ssh -i spark-cluster-virginia.pem -ND 8157 hadoop@ec2-100-24-13-252.compute-1.amazonaws.com
ssh -vvv -i spark-cluster-virginia.pem hadoop@ec2-100-24-13-252.compute-1.amazonaws.com

For this example, I just used the bucket that we do have access to: s3://udacity-dend/log_data/2018/11/2018-11-01-events.json

chmod 600 spark-cluster-virginia.pem # read and write access