using UnityEngine;

namespace UnitySensors.Utils
{
    public class FrameRateSettings : MonoBehaviour
    {
        [SerializeField]
        [Tooltip("Target frame rate. Set to -1 for unlimited.")]
        private int _targetFrameRate = 60;

        [SerializeField]
        [Range(0, 2)]
        [Tooltip("0: Don't Sync, 1: Every VBlank, 2: Every Second VBlank")]
        private int _vSyncCount = 0;

        private void Awake()
        {
            ApplySettings();
        }

        public void ApplySettings()
        {
            QualitySettings.vSyncCount = _vSyncCount;
            Application.targetFrameRate = _targetFrameRate;
        }
    }
}
