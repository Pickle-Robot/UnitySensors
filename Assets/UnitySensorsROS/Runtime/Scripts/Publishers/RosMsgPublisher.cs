using UnityEngine;
using Unity.Robotics.ROSTCPConnector;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;

using UnitySensors.ROS.Serializer;

namespace UnitySensors.ROS.Publisher
{
    public class RosMsgPublisher<T, TT> : MonoBehaviour where T : RosMsgSerializer<TT> where TT : Message, new()
    {
        [SerializeField]
        private float _frequency = 10.0f;

        [SerializeField]
        protected string _topicName;

        [SerializeField]
        protected T _serializer;

        protected ROSConnection _ros;

        private float _dt;

        private float _frequency_inv;

        protected virtual void Start()
        {
            _dt = 0.0f;
            if (_frequency <= 0.0f)
            {
                _frequency = 0.0f;
                _frequency_inv = 0.0f;
            }
            else
            {
                _frequency_inv = 1.0f / _frequency;
            }

            _ros = ROSConnection.GetOrCreateInstance();
            _ros.RegisterPublisher<TT>(_topicName);

            _serializer.Init();
        }

        protected virtual void Update()
        {
            if (_frequency_inv == 0.0f) return;

            _dt += Time.deltaTime;
            if (_dt < _frequency_inv) return;

            Publish();

            _dt -= _frequency_inv;
        }

        public void Publish()
        {
            _ros.Publish(_topicName, _serializer.Serialize());
        }

        private void OnDestroy()
        {
            _serializer.OnDestroy();
        }
    }
}
