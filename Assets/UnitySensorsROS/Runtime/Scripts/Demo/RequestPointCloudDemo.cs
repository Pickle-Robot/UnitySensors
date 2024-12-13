using UnityEngine;
using Unity.Robotics.ROSTCPConnector;
using RosMessageTypes.Std;
using UnitySensors.Sensor.Camera;
using UnitySensors.ROS.Publisher.Sensor;


namespace UnitySensors.ROS
{
    public class RequestPointCloudDemo : MonoBehaviour
    {
        [SerializeField]
        private string _requestTopic = "";

        [SerializeField]
        private DepthCameraSensor _sensor;

        [SerializeField]
        private DepthCameraPointCloud2MsgPublisher _publisher;

        private ROSConnection _ros;

        private void Awake()
        {
            _ros = ROSConnection.GetOrCreateInstance();
            _ros.Subscribe<HeaderMsg>(_requestTopic, RequestCallback);
        }

        private void RequestCallback(HeaderMsg msg)
        {
            _sensor.ForceUpdateSensor();
            _publisher.Publish();
        }
    }
}