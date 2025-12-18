using UnityEngine;
using Unity.Robotics.ROSTCPConnector;

using UnitySensors.Sensor.LiDAR;
using UnitySensors.DataType.Sensor.PointCloud;

namespace UnitySensors.ROS.Publisher.Sensor
{
    public class LiDAR360PointCloud2MsgPublisher : PointCloud2MsgPublisher<PointXYZI>
    {
        [SerializeField]
        private RaycastLiDARSensor360 _source;

        private ROSConnection _ros;

        private void Awake()
        {
            if (_source != null)
            {
                _serializer.SetSource(_source);
            }
        }

        protected override void Start()
        {
            base.Start();
            _ros = ROSConnection.GetOrCreateInstance();

            if (_source != null)
            {
                _source.onScanCompleted += OnScanCompleted;
            }
        }

        protected override void Update()
        {
            // Override base Update to disable timer-based publishing
        }

        private void OnScanCompleted()
        {
            if (_ros != null && _serializer != null)
            {
                _ros.Publish(_topicName, _serializer.Serialize());
            }
        }

        private void OnDestroy()
        {
            if (_source != null)
            {
                _source.onScanCompleted -= OnScanCompleted;
            }

            // Replicating base.OnDestroy() cleanup since it is private in RosMsgPublisher
            if (_serializer != null)
            {
                _serializer.OnDestroy();
            }
        }
    }
}

