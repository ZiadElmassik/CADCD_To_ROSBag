import sys
import tf
import os
import cv2
import rospy
import rosbag
from datetime import datetime
from std_msgs.msg import Header
from sensor_msgs.msg import CameraInfo, Imu, PointField, NavSatFix
import sensor_msgs.point_cloud2 as pcl2
from geometry_msgs.msg import TransformStamped, TwistStamped, Transform
from cv_bridge import CvBridge
import numpy as np
import argparse
import progressbar
from pathvalidate import sanitize_filepath

# --------------------------Save camera images as messages---------------------------------------

def save_camera_data(bag, directory , bridge, camera_frame_id, topic, initial_time): 
	print("Exporting Camera 00")
	
# Joining directories to access images for camera 00 and timestamps
	image_dir = os.path.join(directory, 'image_00')
	image_path = os.path.join(image_dir, 'data')
	image_filenames = sorted(os.listdir(image_path))
	with open(os.path.join(image_dir, 'timestamps.txt')) as f:
		image_datetimes = map(lambda x: datetime.strptime(x[:-4], '%Y-%m-%d %H:%M:%S.%f'), f.readlines())
	
# Creating CameraInfo Topic
	calib = CameraInfo()
	calib.header.frame_id = camera_frame_id
	
# Data Obtained from calib/00.yaml
	calib.width = 1280
	calib.height = 1024
	calib.distortion_model = 'plumb_bob'
	calib.D = np.array([-0.211078226790761, 0.101157542400588, -0.000329515817247860, 0.000330423801388672, -0.0232053947325804])
	calib.K = np.array([653.956033188809, -0.235925653043616, 653.221172545916, 0, 655.540088617960, 508.73286399391, 0, 0, 1])
	calib.R = np.array([1, 0, 0, 0, 1, 0, 0, 0, 1])
	calib.P = np.array([653.956033188809, -0.235925653043616, 653.221172545916, 0, 0, 655.540088617960, 508.73286399391, 0, 0, 0, 1, 0])
	
# Iterating over images
	iterable = zip(image_datetimes, image_filenames)
	bar = progressbar.ProgressBar()
	for dt, filename in bar(iterable):
		image_filename = os.path.join(image_path, filename)
		cv_image = cv2.imread(image_filename)
		calib.height, calib.width = cv_image.shape[:2]
		encoding = "bgr8"
		
# Converting images to messages
		image_message = bridge.cv2_to_imgmsg(cv_image, encoding=encoding)
		image_message.header.frame_id = camera_frame_id
		image_message.header.stamp = rospy.Time.from_sec(float(datetime.strftime(dt, "%s.%f")))
		
# Preparing topic 
		topic_ext = "/image_raw"
		calib.header.stamp = image_message.header.stamp
		bag.write(topic + topic_ext, image_message, t = image_message.header.stamp)
		bag.write(topic + '/camera_info', calib, t = calib.header.stamp) 
		
#---------------------------Save LiDAR PointClouds as messages----------------------------------
		
def save_velo_data(bag, directory, velo_frame_id, topic):
	print("Exporting velodyne data")
	velo_path = os.path.join(directory, 'lidar_points')
	velo_data_dir = os.path.join(velo_path, 'data')
	velo_filenames = sorted(os.listdir(velo_data_dir))
	with open(os.path.join(velo_path, 'timestamps.txt')) as f: # get the timestamps
		lines = f.readlines()
		velo_datetimes = []
		for line in lines:
			if len(line) == 1:
				continue
			dt = datetime.strptime(line[:-4], '%Y-%m-%d %H:%M:%S.%f') # formatting the time
			print(dt)
			velo_datetimes.append(dt)
	iterable = zip(velo_datetimes, velo_filenames)
	bar = progressbar.ProgressBar()
	for dt, filename in bar(iterable):
		if dt is None:
			continue
		velo_filename = os.path.join(velo_data_dir, filename)
		print(velo_filename)
		
# Read binary data
		scan = (np.fromfile(velo_filename, dtype=np.float32)).reshape(-1, 4) 
		
# Create header
		header = Header()
		header.frame_id = velo_frame_id
		header.stamp = rospy.Time.from_sec(float(datetime.strftime(dt, "%s.%f")))

# Fill PCL msg
		fields = [PointField('x', 0, PointField.FLOAT32, 1),
                    PointField('y', 4, PointField.FLOAT32, 1),
                    PointField('z', 8, PointField.FLOAT32, 1),
                    PointField('intensity', 12, PointField.FLOAT32, 1)]
		pcl_msg = pcl2.create_cloud(header, fields, scan)
		print(pcl_msg.header.stamp)
		bag.write(topic + '/pointcloud', pcl_msg, t=pcl_msg.header.stamp)

def main():
	
	available_dates = ["2018_03_06", "2018_03_07", "2019_02_27"]
	
	image_sequences = []
	for s in range(83):
		image_sequences.append(str(s).zfill(4))
				
	parser = argparse.ArgumentParser(description="Convert CADCD Camera 00 Images and PCL Data to ROS bag file the easy way!")
	parser.add_argument("dir", nargs = "?", default = sanitize_filepath(os.getcwd()), help = "base directory of the dataset, if no directory passed the deafult is current working directory")
	parser.add_argument("-t", "--date", choices = available_dates, help = "date of the raw dataset (i.e. 2019_02_27), option is only for RAW datasets.")
	parser.add_argument("-s", "--sequence", choices = image_sequences, help = "sequence of the dataset (between 1 - 82 with some missing numbers in between)")
	parser.add_argument("-f", "--frame",
                        help="frame number of the raw dataset (i.e. 000000001) or all to do everything ")
	args = parser.parse_args()
	bridge = CvBridge()
	compression = rosbag.Compression.NONE
	
	camera = (0, 'camera_00', 'CADCD/camera_00')
	if args.date == None:
		print("Date option is not given. Please specify date of dataset.")
		sys.exit(1)
	if args.sequence == None:
		print("Sequence option is not given. Please specify which sequence should be played.")
		sys.exit(1)
	
	directory = sanitize_filepath(args.dir + '/' + args.date + '/' + args.sequence + '/labeled')
	if not os.path.exists(directory):
		print('Path {} does not exist. Exiting.'.format(directory))
		sys.exit(1)
	
	
	bag = rosbag.Bag("CADCD_{}_seq_{}".format(args.date, args.sequence), 'w', compression=compression)		
	
	try:
	
		velo_frame_id = "velo_link"
		velo_topic = "cadcd/velo"
		camera_topic= "/cadcd/{}/{}/camera_00".format(args.date, args.sequence)
		save_camera_data(bag, directory, bridge, "camera_00", camera_topic, initial_time=None)
		save_velo_data(bag, directory, velo_frame_id, velo_topic)
	
	finally:
		print("## OVERVIEW ##")
		print(bag)
		bag.close()

if __name__ == '__main__':
    main()	
	
	
