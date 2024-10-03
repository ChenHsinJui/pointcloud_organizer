#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import logging
import rosbag2_py
import numpy as np
from operator import itemgetter
from array import array

from sensor_msgs.msg import PointCloud2, PointField
from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

logging.basicConfig(level=logging.DEBUG)

class OrganizeBag(object):
    def __init__(self, bag, topics):
        self._bag = bag
        self._topics = topics
        self._reader = rosbag2_py.SequentialReader()
        self._writer = rosbag2_py.SequentialWriter()
        self._horizontal_resol = 175
        self._point_id = None

    def echo_bag(self):
        storage_options = rosbag2_py._storage.StorageOptions(uri=self._bag, storage_id='sqlite3')
        converter_options = rosbag2_py._storage.ConverterOptions('', '')
        self._reader.open(storage_options, converter_options)
        if self._topics:
            storage_filter = rosbag2_py._storage.StorageFilter(topics=self._topics)
            self._reader.set_filter(storage_filter)
        topic_name2type = {topic_metadata.name: topic_metadata.type for topic_metadata in self._reader.get_all_topics_and_types()}

        msg_cnts = {}        
        while self._reader.has_next():
            (topic_name, data, t) = self._reader.read_next()

            if topic_name not in msg_cnts:  
                msg_cnts[topic_name] = 1
            else:
                msg_cnts[topic_name] += 1            

            # message_type = get_message(topic_name2type[topic_name])
            # msg = deserialize_message(data, message_type)

            # print("----------[%s : %s]---------" % (topic_name, topic_name2type[topic_name]))
            # # print(len(msg.data))
            # # print(msg.row_step)
            # print("Header: {}\nHeight: {}\nWidth: {}\nPointField: {}\npoint_step: {}\nrow_step: {}"
            #       .format(msg.header, msg.height, msg.width, msg.fields, msg.point_step, msg.row_step))

        logging.info("----------[rosbag summary]---------")
        for topic_name in msg_cnts:  
            logging.info("topic %s msg cnt is %d" %(topic_name, msg_cnts[topic_name]))
    def to_nano(self,stamp):
        return stamp.sec * 1000000000 + stamp.nanosec
    def convert_from_byte(self,target_points):
        points = []
        for point in target_points:
            if self._point_id is None:
                self._point_id = point[3] # Restore point id in list of uint8 format
            temp = [point[0], point[1], point[2]] # x, y, and z are stored in uint8 array format
            uint_point = np.array(temp, dtype=np.uint8)
            binary_data = uint_point.tobytes()
            float_point = np.frombuffer(binary_data, dtype=np.float32)
            points.append(list(float_point))

        return points

    def convert_to_byte(self,target_points): # TODO: encoding may have some issue the output row step will be bigger than before
        data = []
        for point in target_points:
            for ele in point:
                float_data = np.array(ele, dtype=np.float32)
                binary_data = float_data.tobytes()
                uint_data = np.frombuffer(binary_data, dtype=np.uint8)
                
                data.extend(list(uint_data))
                data.extend(self._point_id)
        return array('B',data)

    def rip_out_redundant(self, points):
        num_points = len(points)
        x_getter = itemgetter(0)
        z_getter = itemgetter(2)
        iterator_getter = {0: lambda points_array: max(points_array, key=x_getter), 1: lambda points_array: max(points_array, key=z_getter), 
                           2: lambda points_array: min(points_array, key=x_getter), 3: lambda points_array: min(points_array, key=z_getter)}
        height = num_points//self._horizontal_resol
        res = num_points%self._horizontal_resol
        for i in range(res):
            getter = iterator_getter[i%4]
            points.remove(getter(points))
        assert len(points)!=num_points or res==0, "You didn't rip out any points"
        return points, height


    def generate_pc_data(self, pc_data, row_step, point_step):
        points_data = [pc_data[i:i+point_step] for i in range(0,row_step,point_step)]

        target_data = []
        for point in points_data:
            splitted_point = [point[i:i+4] for i in range(0,len(point),4)] # Assume that all field take 4 bytes in size.
            target_data.append(splitted_point)
        assert len(target_data) == int(row_step/point_step), "The length of splitted points didn't match the number from row step."
        
        points = self.convert_from_byte(target_data)
        assert len(points) == len(target_data), "The length of decoded points array didn't match the length of the given splitted points arrray."
        print(points[0])
        ### TODO: points processing, contains height calculation
        result_points, height = self.rip_out_redundant(points)
        ###
        data = self.convert_to_byte(result_points)
        return data, len(data), height

    def get_pointcloud(self,unorganized_msg):
        msg = PointCloud2()
        msg.header = unorganized_msg.header
        msg.fields = unorganized_msg.fields
        msg.point_step = unorganized_msg.point_step
        msg.width = self._horizontal_resol

        msg.data, msg.row_step, msg.height = self.generate_pc_data(unorganized_msg.data, unorganized_msg.row_step, unorganized_msg.point_step)
        print("the row step of unorganized msg is {}, while after organizing the row step is {}".format(unorganized_msg.row_step, msg.row_step))
        msg.is_dense = unorganized_msg.is_dense
        return msg

    def convert_to_organized(self,output_path):
        storage_options = rosbag2_py._storage.StorageOptions(uri=self._bag, storage_id='sqlite3')
        converter_options = rosbag2_py._storage.ConverterOptions('', '')
        self._reader.open(storage_options, converter_options)
        if self._topics:
            storage_filter = rosbag2_py._storage.StorageFilter(topics=self._topics)
            self._reader.set_filter(storage_filter)
        topic_name2type = {topic_metadata.name: topic_metadata.type for topic_metadata in self._reader.get_all_topics_and_types()}

        ### open rosbag writer
        self._writer.open(
            rosbag2_py.StorageOptions(uri=str(output_path), storage_id="sqlite3"),
            rosbag2_py.ConverterOptions(
                input_serialization_format="cdr", output_serialization_format="cdr"
            ),
        )
        self._writer.create_topic(rosbag2_py.TopicMetadata(
            name="/sensing/lidar/bf_lidar/points_raw", type="sensor_msgs/msg/PointCloud2", serialization_format="cdr"
        ))
        self._writer.create_topic(rosbag2_py.TopicMetadata(
            name="/sensing/imu/imu_raw", type="sensor_msgs/msg/Imu", serialization_format="cdr"
        ))
        while self._reader.has_next():
            (topic_name, data, t) = self._reader.read_next()           

            message_type = get_message(topic_name2type[topic_name])
            msg = deserialize_message(data, message_type)
            if "lidar" in topic_name:
                output_msg = self.get_pointcloud(msg)
            else:
                output_msg = msg
            self._writer.write(topic_name,serialize_message(output_msg),self.to_nano(msg.header.stamp))
        del self._writer



def main():
  parser = argparse.ArgumentParser(description="echo ros2 bag topic")
  parser.add_argument("-b", "--bag", type=str, required=True, help="specify rosbag")
  parser.add_argument("-e","--echo",help="whether you want to echo the bag info or not",action="store_true")
  parser.add_argument("-o", "--output_path", type=str, required=True, help="specify the output path to store")
  parser.add_argument("-t", "--topics", nargs="+", type=str, help="specify topic")
  parser.add_argument('extra_args', nargs='*', help=argparse.SUPPRESS)
  args=parser.parse_args()

  if not os.path.isfile(args.bag):
      logging.error("%s is not found!" %args.bag)
      return
  if os.path.isfile(args.output_path):
      logging.error("%s exist, pick another file path" %args.output)
      return
  bag = OrganizeBag(args.bag, args.topics)
  
  bag.convert_to_organized(args.output_path)
  logging.info("------- finish converting!! ------")
  if args.echo:
    bag.echo_bag()

if __name__ == "__main__":
    main()
