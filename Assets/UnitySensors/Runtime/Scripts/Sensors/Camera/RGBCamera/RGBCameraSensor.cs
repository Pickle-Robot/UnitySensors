using UnityEngine;
using UnityEngine.Rendering;

using UnitySensors.Interface.Sensor;

namespace UnitySensors.Sensor.Camera
{
    public class RGBCameraSensor : CameraSensor, ITextureInterface
    {
        private UnityEngine.Camera _camera;
        private RenderTexture _rt = null;
        private Texture2D _texture;

        public override UnityEngine.Camera m_camera { get => _camera; }
        public Texture2D texture0 { get => _texture; }
        public Texture2D texture1 { get => _texture; }

        protected override void Init()
        {
            _camera = GetComponent<UnityEngine.Camera>();
            _camera.fieldOfView = _fov;

            // sRGB read/write so the GPU gamma-encodes on write in a linear color space project;
            // a float render texture would stay linear and every published image would look dark.
            _rt = new RenderTexture(_resolution.x, _resolution.y, 24, RenderTextureFormat.ARGB32, RenderTextureReadWrite.sRGB);
            _camera.targetTexture = _rt;

            _texture = new Texture2D(_resolution.x, _resolution.y, TextureFormat.RGBA32, false);
        }

        protected override void UpdateSensor()
        {
            if (!LoadTexture()) return;

            if (onSensorUpdated != null)
                onSensorUpdated.Invoke();
        }

        protected bool LoadTexture()
        {
            bool result = false;
            AsyncGPUReadback.Request(_rt, 0, TextureFormat.RGBA32, request => {
                if (request.hasError)
                {
                }
                else
                {
                    var data = request.GetData<Color32>();
                    _texture.LoadRawTextureData(data);
                    _texture.Apply();
                    result = true;
                }
            });
            AsyncGPUReadback.WaitAllRequests();
            return result;
        }

        protected override void OnSensorDestroy()
        {
            _rt.Release();
        }
    }
}
