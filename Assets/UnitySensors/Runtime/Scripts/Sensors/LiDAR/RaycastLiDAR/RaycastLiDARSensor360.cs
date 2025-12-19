using System;
using System.Reflection;
using UnityEngine;

using Unity.Burst;
using Unity.Collections;
using Unity.Jobs;
using Unity.Mathematics;

using UnitySensors.DataType.Sensor;
using UnitySensors.DataType.Sensor.PointCloud;
using UnitySensors.Utils.Noise;

using Random = Unity.Mathematics.Random;

namespace UnitySensors.Sensor.LiDAR
{
    public class RaycastLiDARSensor360 : LiDARSensor
    {
        [SerializeField]
        private float _scanFrequency = 10.0f; // Hz

        private Transform _transform;

        private JobHandle _jobHandle;

        private IUpdateRaycastCommandsJob _updateRaycastCommandsJob;
        private IUpdateGaussianNoisesJob _updateGaussianNoisesJob;
        private IRaycastHitsToPointsJob _raycastHitsToPointsJob;
        private IPointsTransformJob _pointsTransformJob;

        private NativeArray<float3> _directions;
        private NativeArray<RaycastCommand> _raycastCommands;
        private NativeArray<RaycastHit> _raycastHits;

        private NativeArray<float> _noises;

        private int _batchSize;
        private NativeArray<PointXYZI> _batchPoints;

        private PointCloud<PointXYZI> _worldPointCloud; // Accumulator (World Space)
        private PointCloud<PointXYZI> _localPointCloud; // Output (Local Space)
        private int _currentScanIndex = 0;

        public event Action onScanCompleted;

        /// <summary>
        /// Returns the accumulated point cloud containing the full 360 scan in Local Space.
        /// </summary>
        public override PointCloud<PointXYZI> pointCloud { get => _worldPointCloud; }
        public override int pointsNum { get => scanPattern.size; }

        public float ScanFrequency
        {
            get => _scanFrequency;
            set
            {
                _scanFrequency = value;
                ApplyScanFrequency();
            }
        }

        protected override void Init()
        {
            base.Init();

            // Auto-calculate batch size to support desired scan frequency
            // Assume at least 60 FPS. Ensure we can complete a scan in reasonable number of frames.
            // Target completing a scan in ~4 frames at 60 FPS to allow for overhead and high freq scans.
            int minBatch = Mathf.CeilToInt(scanPattern.size / 4.0f);
            _batchSize = Mathf.Max(pointsNumPerScan, minBatch);

            ApplyScanFrequency();

            _transform = this.transform;

            LoadScanData();
            SetupJobs();
        }

        private void ApplyScanFrequency()
        {
            float totalPoints = scanPattern.size;
            float batchSize = _batchSize;

            if (batchSize <= 0) return;

            float targetUpdateFreq = _scanFrequency * (totalPoints / batchSize);

            // Reflection to set _frequency and _frequency_inv in UnitySensor base class
            var type = typeof(UnitySensor);
            var freqField = type.GetField("_frequency", BindingFlags.Instance | BindingFlags.NonPublic);
            if (freqField != null) freqField.SetValue(this, targetUpdateFreq);

            var freqInvField = type.GetField("_frequency_inv", BindingFlags.Instance | BindingFlags.NonPublic);
            if (freqInvField != null) freqInvField.SetValue(this, 1.0f / targetUpdateFreq);
        }

        private void LoadScanData()
        {
            _directions = new NativeArray<float3>(scanPattern.size * 2, Allocator.Persistent);
            for (int i = 0; i < scanPattern.size; i++)
            {
                _directions[i] = _directions[i + scanPattern.size] = scanPattern.scans[i];
            }

            // Buffer for World Space accumulation
            _worldPointCloud = new PointCloud<PointXYZI>()
            {
                points = new NativeArray<PointXYZI>(scanPattern.size, Allocator.Persistent)
            };

            // Buffer for Local Space output
            _localPointCloud = new PointCloud<PointXYZI>()
            {
                points = new NativeArray<PointXYZI>(scanPattern.size, Allocator.Persistent)
            };
        }

        private void SetupJobs()
        {
            _raycastCommands = new NativeArray<RaycastCommand>(_batchSize, Allocator.Persistent);
            _raycastHits = new NativeArray<RaycastHit>(_batchSize, Allocator.Persistent);
            _noises = new NativeArray<float>(_batchSize, Allocator.Persistent);
            _batchPoints = new NativeArray<PointXYZI>(_batchSize, Allocator.Persistent);

            _updateRaycastCommandsJob = new IUpdateRaycastCommandsJob()
            {
                origin = _transform.position,
                localToWorldMatrix = _transform.localToWorldMatrix,
                maxRange = maxRange,
                directions = _directions,
                indexOffset = 0,
                raycastCommands = _raycastCommands
            };

            _updateGaussianNoisesJob = new IUpdateGaussianNoisesJob()
            {
                sigma = gaussianNoiseSigma,
                random = new Random((uint)Environment.TickCount),
                noises = _noises
            };

            // Calculate points in World Space directly
            _raycastHitsToPointsJob = new IRaycastHitsToPointsJob()
            {
                minRange = minRange,
                sqrMinRange = minRange * minRange,
                maxRange = maxRange,
                maxIntensity = maxIntensity,
                directions = _directions,
                indexOffset = 0,
                raycastHits = _raycastHits,
                noises = _noises,
                points = _batchPoints, // Stores the batch slice
                outWorldSpace = false // Reverting to Local Space output to fix the split artifact
            };

            _pointsTransformJob = new IPointsTransformJob()
            {
                input = _worldPointCloud.points,
                output = _localPointCloud.points
            };
        }

        private bool _isJobScheduled = false;

        // Optimized for performance
        protected override void Update()
        {
            if (_isJobScheduled)
            {
                if (_jobHandle.IsCompleted)
                {
                    _jobHandle.Complete();
                    ProcessJobResults();
                    _isJobScheduled = false;
                }
                else
                {
                    return;
                }
            }

            base.Update();
        }

        protected override void UpdateSensor()
        {
            _updateRaycastCommandsJob.origin = _transform.position;
            _updateRaycastCommandsJob.localToWorldMatrix = _transform.localToWorldMatrix;

            // Update the matrix for the HitsToPoints job so it transforms correctly to World Space
            // _raycastHitsToPointsJob.localToWorldMatrix = _transform.localToWorldMatrix;

            JobHandle updateRaycastCommandsJobHandle = _updateRaycastCommandsJob.Schedule(_batchSize, 1);
            JobHandle updateGaussianNoisesJobHandle = _updateGaussianNoisesJob.Schedule(_batchSize, 1, updateRaycastCommandsJobHandle);
            JobHandle raycastJobHandle = RaycastCommand.ScheduleBatch(_raycastCommands, _raycastHits, 256, updateGaussianNoisesJobHandle);
            _jobHandle = _raycastHitsToPointsJob.Schedule(_batchSize, 1, raycastJobHandle);

            JobHandle.ScheduleBatchedJobs();
            _isJobScheduled = true;
        }

        private void ProcessJobResults()
        {
            // Copy results (World Space Slice) from batch buffer to Accumulator (World Space)
            int remaining = scanPattern.size - _currentScanIndex;
            if (_batchSize <= remaining)
            {
                NativeArray<PointXYZI>.Copy(_batchPoints, 0, _worldPointCloud.points, _currentScanIndex, _batchSize);
            }
            else
            {
                // Copy first part to end of buffer
                NativeArray<PointXYZI>.Copy(_batchPoints, 0, _worldPointCloud.points, _currentScanIndex, remaining);
                // Copy second part to start of buffer
                NativeArray<PointXYZI>.Copy(_batchPoints, remaining, _worldPointCloud.points, 0, _batchSize - remaining);
            }

            // Update offsets for next frame
            int nextIndex = _currentScanIndex + _batchSize;
            bool scanComplete = nextIndex >= scanPattern.size;

            _currentScanIndex = nextIndex % scanPattern.size;

            _updateRaycastCommandsJob.indexOffset = _currentScanIndex;
            _raycastHitsToPointsJob.indexOffset = _currentScanIndex;

            if (onSensorUpdated != null)
                onSensorUpdated.Invoke();

            if (scanComplete)
            {
                // Transform the full World Space cloud to Local Space relative to CURRENT transform
                // _pointsTransformJob.matrix = _transform.worldToLocalMatrix;
                // JobHandle transformHandle = _pointsTransformJob.Schedule(scanPattern.size, 64);
                // transformHandle.Complete();

                onScanCompleted?.Invoke();
            }
        }

        protected override void OnSensorDestroy()
        {
            _jobHandle.Complete();
            if (_noises.IsCreated) _noises.Dispose();
            if (_directions.IsCreated) _directions.Dispose();
            if (_raycastCommands.IsCreated) _raycastCommands.Dispose();
            if (_raycastHits.IsCreated) _raycastHits.Dispose();
            if (_batchPoints.IsCreated) _batchPoints.Dispose();

            if (_worldPointCloud != null) _worldPointCloud.Dispose();
            if (_localPointCloud != null) _localPointCloud.Dispose();

            base.OnSensorDestroy();
        }
    }

    [BurstCompile]
    public struct IPointsTransformJob : IJobParallelFor
    {
        [ReadOnly]
        public Matrix4x4 matrix;
        [ReadOnly]
        public NativeArray<PointXYZI> input;

        public NativeArray<PointXYZI> output;

        public void Execute(int index)
        {
            PointXYZI point = input[index];
            point.position = math.mul(matrix, new float4(point.position, 1.0f)).xyz;
            output[index] = point;
        }
    }
}
