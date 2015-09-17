#!/usr/bin/env python

import time
from fluent.sender import FluentSender
import rospy

from rosgraph_msgs.msg import Log

LOG_MSGS = {
    Log.DEBUG: 'debug',
    Log.INFO: 'info',
    Log.WARN: 'warn',
    Log.ERROR: 'error',
    Log.FATAL: 'fatal',
}


def main():
    host='localhost'
    port=24224
    log_sender = FluentSender('ros.log', host, port)

    def log_callback(msg):
        timestamp = msg.header.stamp.to_sec()
        data = {
            'name': msg.name,
            'msg': msg.msg,
            'file': msg.file,
            'function': msg.function,
            'line': msg.line,
        }
        label = LOG_MSGS.get(msg.level, 'trace')
        log_sender.emit_with_time(label, timestamp, data)

    rospy.init_node("fluent_logger", anonymous=True)
    rospy.Subscriber("/rosout_agg", Log, log_callback, queue_size=100)
    rospy.spin()

if __name__ == '__main__':
    main()
