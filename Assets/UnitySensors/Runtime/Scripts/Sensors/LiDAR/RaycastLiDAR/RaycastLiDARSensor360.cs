using System;
using UnityEngine;

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

        [SerializeField, Tooltip("If true, points are accumulated in Local Space relative to the sensor pose at the time of capture, creating a rolling shutter distortion effect when the sensor moves. If false, points are accumulated in World Space (effectively deskewed relative to the Map frame).")]
        private bool _enableRollingShutter = false;

        private Transform _transform;

        private JobHandle _jobHandle;

        private IUpdateRaycastCommandsJob _updateRaycastCommandsJob;
        private IUpdateGaussianNoisesJob _updateGaussianNoisesJob;
        private IRaycastHitsToPointsJob _raycastHitsToPointsJob;

        private NativeArray<float3> _directions;
        private NativeArray<RaycastCommand> _raycastCommands;
        private NativeArray<RaycastHit> _raycastHits;

        private NativeArray<float> _noises;

        private PointCloud<PointXYZI> _fullPointCloud; // Accumulator
        private int _currentScanIndex = 0;

        public event Action onScanCompleted;

        /// <summary>
        /// Returns the accumulated point cloud containing the full 360 scan in Local Space.
        /// </summary>
        public override PointCloud<PointXYZI> pointCloud { get => _fullPointCloud; }
        public override int pointsNum { get => scanPattern.size; }

        public float ScanFrequency
        {
            get => _scanFrequency;
            set
            {
                _scanFrequency = value;
            }
        }

        public bool EnableRollingShutter
        {
            get => _enableRollingShutter;
            set => _enableRollingShutter = value;
        }

        protected override void Init()
        {
            base.Init();

            _transform = this.transform;

            LoadScanData();
            SetupJobs();
        }

        private void LoadScanData()
        {
            _directions = new NativeArray<float3>(scanPattern.size * 2, Allocator.Persistent);
            for (int i = 0; i < scanPattern.size; i++)
            {
                _directions[i] = _directions[i + scanPattern.size] = scanPattern.scans[i];
            }

            // Buffer for accumulation (Local Space)
            _fullPointCloud = new PointCloud<PointXYZI>()
            {
                points = new NativeArray<PointXYZI>(scanPattern.size, Allocator.Persistent)
            };
        }

        private void SetupJobs()
        {
            // Allocate Max Capacity (Full Scan Size) to allow large variable batches
            int maxCapacity = scanPattern.size;
            _raycastCommands = new NativeArray<RaycastCommand>(maxCapacity, Allocator.Persistent);
            _raycastHits = new NativeArray<RaycastHit>(maxCapacity, Allocator.Persistent);
            _noises = new NativeArray<float>(maxCapacity, Allocator.Persistent);

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

            // Calculate points in Local Space directly (outWorldSpace = false)
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
                points = _fullPointCloud.points, // Write directly to Full Cloud (using Slice)
                outWorldSpace = !_enableRollingShutter
            };
        }

        protected override void Update()
        {
            // Calculate how many points to process THIS frame to match frequency
            float pointsPerSecond = scanPattern.size * _scanFrequency;
            float pointsToProcessFloat = pointsPerSecond * Time.deltaTime;
            int pointsToProcess = Mathf.CeilToInt(pointsToProcessFloat);

            // Clamp to full scan size
            pointsToProcess = Mathf.Min(pointsToProcess, scanPattern.size);

            if (pointsToProcess <= 0) return;

            // Prepare Jobs
            _updateRaycastCommandsJob.origin = _transform.position;
            _updateRaycastCommandsJob.localToWorldMatrix = _transform.localToWorldMatrix;
            _updateRaycastCommandsJob.indexOffset = _currentScanIndex;

            // Update the matrix for the HitsToPoints job so it transforms correctly to World Space
            _raycastHitsToPointsJob.localToWorldMatrix = _transform.localToWorldMatrix;
            _raycastHitsToPointsJob.outWorldSpace = !_enableRollingShutter;

            // Schedule Raycast Command Generation (Burst)
            // Note: We need to set the internal raycastCommands reference again if the job struct was copied?
            // No, NativeArrays are reference types (structs containing pointers).
            // However, the error says: "The UNKNOWN_OBJECT_TYPE IUpdateRaycastCommandsJob.raycastCommands has not been assigned or constructed."
            // This suggests that the NativeArray inside the job struct is invalid.

            // Re-assign the NativeArray references every frame to be safe.
            _updateRaycastCommandsJob.raycastCommands = _raycastCommands;
            _updateRaycastCommandsJob.directions = _directions;

            // Pass _jobHandle from previous frame/batch (which is complete) to satisfy safety system
            JobHandle commandsHandle = _updateRaycastCommandsJob.Schedule(pointsToProcess, 64, _jobHandle);

            // Schedule Noise Generation (Burst)
            JobHandle noiseHandle = _updateGaussianNoisesJob.Schedule(pointsToProcess, 64, _jobHandle);

            // Combine Dependencies
            JobHandle dep = JobHandle.CombineDependencies(commandsHandle, noiseHandle);

            // Schedule Physics Raycasts (Physics Engine)
            NativeArray<RaycastCommand> commandsSlice = _raycastCommands.GetSubArray(0, pointsToProcess);
            NativeArray<RaycastHit> hitsSlice = _raycastHits.GetSubArray(0, pointsToProcess);

            JobHandle raycastHandle = RaycastCommand.ScheduleBatch(commandsSlice, hitsSlice, 256, dep);

            // Schedule Hits -> Points (Burst)
            // We need to handle wrapping manually since we are writing to a Ring Buffer (_fullPointCloud)

            int remaining = scanPattern.size - _currentScanIndex;

            if (pointsToProcess <= remaining)
            {
                // No wrapping
                var pointsSlice = _fullPointCloud.points.GetSubArray(_currentScanIndex, pointsToProcess);

                _raycastHitsToPointsJob.indexOffset = _currentScanIndex;
                _raycastHitsToPointsJob.raycastHits = hitsSlice;
                _raycastHitsToPointsJob.points = pointsSlice;

                _jobHandle = _raycastHitsToPointsJob.Schedule(pointsToProcess, 64, raycastHandle);
            }
            else
            {
                // Wrapping: Split into two jobs
                int firstPart = remaining;
                int secondPart = pointsToProcess - remaining;

                // Job 1: End of buffer
                var hitsSlice1 = hitsSlice.GetSubArray(0, firstPart);
                var pointsSlice1 = _fullPointCloud.points.GetSubArray(_currentScanIndex, firstPart);

                _raycastHitsToPointsJob.indexOffset = _currentScanIndex;
                _raycastHitsToPointsJob.raycastHits = hitsSlice1;
                _raycastHitsToPointsJob.points = pointsSlice1;

                JobHandle h1 = _raycastHitsToPointsJob.Schedule(firstPart, 64, raycastHandle);

                // Job 2: Start of buffer
                var hitsSlice2 = hitsSlice.GetSubArray(firstPart, secondPart);
                var pointsSlice2 = _fullPointCloud.points.GetSubArray(0, secondPart);

                // Note: The index offset for directions needs to account for the wrap.
                _raycastHitsToPointsJob.indexOffset = _currentScanIndex + firstPart;
                _raycastHitsToPointsJob.raycastHits = hitsSlice2;
                _raycastHitsToPointsJob.points = pointsSlice2;

                _jobHandle = _raycastHitsToPointsJob.Schedule(secondPart, 64, h1);
            }

            JobHandle.ScheduleBatchedJobs();

            // Synchronous Wait (Fixes Race Condition with Serializer)
            _jobHandle.Complete();

            // Advance Index
            int nextIndex = _currentScanIndex + pointsToProcess;
            bool scanComplete = nextIndex >= scanPattern.size;
            _currentScanIndex = nextIndex % scanPattern.size;

            if (onSensorUpdated != null)
                onSensorUpdated.Invoke();

            if (scanComplete)
            {
                onScanCompleted?.Invoke();
            }
        }

        protected override void UpdateSensor()
        {
            // Unused in this implementation, logic moved to Update()
        }

        protected override void OnSensorDestroy()
        {
            _jobHandle.Complete();
            if (_noises.IsCreated) _noises.Dispose();
            if (_directions.IsCreated) _directions.Dispose();
            if (_raycastCommands.IsCreated) _raycastCommands.Dispose();
            if (_raycastHits.IsCreated) _raycastHits.Dispose();

            if (_fullPointCloud != null) _fullPointCloud.Dispose();

            base.OnSensorDestroy();
        }
    }
}
