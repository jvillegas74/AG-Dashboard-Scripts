--exec dbo.SP_DBA_CurrentlyExec @databases=1;
--exec dbo.SP_WhoIsActive
DROP TABLE IF EXISTS #tmpag_availability_groups
DROP TABLE IF EXISTS #tmpag_availability_replicas
DROP TABLE IF EXISTS #tmpag_availability_replica_states
DROP TABLE IF EXISTS #tmpag_availability_groups
DROP TABLE IF EXISTS #tmpag_availability_group_states
DROP TABLE IF EXISTS #tmpdbr_database_replica_state
DROP TABLE IF EXISTS #tmpdbr_availability_replica_states
DROP TABLE IF EXISTS #tmpdbr_database_replica_states
DROP TABLE IF EXISTS #tmpdbr_availability_replicas
DROP TABLE IF EXISTS #tmpdbr_database_replica_states_primary_LCT
DROP TABLE IF EXISTS #tmpdbr_database_replica_cluster_states
GO


        declare @HkeyLocal nvarchar(18)
        declare @ServicesRegPath nvarchar(34)
        declare @SqlServiceRegPath sysname
        declare @BrowserServiceRegPath sysname
        declare @MSSqlServerRegPath nvarchar(31)
        declare @InstanceNamesRegPath nvarchar(59)
        declare @InstanceRegPath sysname
        declare @SetupRegPath sysname
        declare @NpRegPath sysname
        declare @TcpRegPath sysname
        declare @RegPathParams sysname
        declare @FilestreamRegPath sysname

        select @HkeyLocal=N'HKEY_LOCAL_MACHINE'

        -- Instance-based paths
        select @MSSqlServerRegPath=N'SOFTWARE\Microsoft\MSSQLServer'
        select @InstanceRegPath=@MSSqlServerRegPath + N'\MSSQLServer'
        select @FilestreamRegPath=@InstanceRegPath + N'\Filestream'
        select @SetupRegPath=@MSSqlServerRegPath + N'\Setup'
        select @RegPathParams=@InstanceRegPath+'\Parameters'

        -- Services
        select @ServicesRegPath=N'SYSTEM\CurrentControlSet\Services'
        select @SqlServiceRegPath=@ServicesRegPath + N'\MSSQLSERVER'
        select @BrowserServiceRegPath=@ServicesRegPath + N'\SQLBrowser'

        -- InstanceId setting
        select @InstanceNamesRegPath=N'SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL'

        -- Network settings
        select @NpRegPath=@InstanceRegPath + N'\SuperSocketNetLib\Np'
        select @TcpRegPath=@InstanceRegPath + N'\SuperSocketNetLib\Tcp'
      


        declare @SmoAuditLevel int
        exec master.dbo.xp_instance_regread @HkeyLocal, @InstanceRegPath, N'AuditLevel', @SmoAuditLevel OUTPUT
      


        declare @NumErrorLogs int
        exec master.dbo.xp_instance_regread @HkeyLocal, @InstanceRegPath, N'NumErrorLogs', @NumErrorLogs OUTPUT
      


        declare @SmoLoginMode int
        exec master.dbo.xp_instance_regread @HkeyLocal, @InstanceRegPath, N'LoginMode', @SmoLoginMode OUTPUT
      


        declare @SmoMailProfile nvarchar(512)
        exec master.dbo.xp_instance_regread @HkeyLocal, @InstanceRegPath, N'MailAccountName', @SmoMailProfile OUTPUT
      


        declare @BackupDirectory nvarchar(512)
        if 1=isnull(cast(SERVERPROPERTY('IsLocalDB') as bit), 0)
        select @BackupDirectory=cast(SERVERPROPERTY('instancedefaultdatapath') as nvarchar(512))
        else
        exec master.dbo.xp_instance_regread @HkeyLocal, @InstanceRegPath, N'BackupDirectory', @BackupDirectory OUTPUT
      


        declare @SmoPerfMonMode int
        exec master.dbo.xp_instance_regread @HkeyLocal, @InstanceRegPath, N'Performance', @SmoPerfMonMode OUTPUT

        if @SmoPerfMonMode is null
        begin
        set @SmoPerfMonMode = 1000
        end
      


        declare @InstallSqlDataDir nvarchar(512)
        exec master.dbo.xp_instance_regread @HkeyLocal, @SetupRegPath, N'SQLDataRoot', @InstallSqlDataDir OUTPUT
      


        declare @MasterPath nvarchar(512)
        declare @LogPath nvarchar(512)
        declare @ErrorLog nvarchar(512)
        declare @ErrorLogPath nvarchar(512)
        declare @Slash varchar = convert(varchar, serverproperty('PathSeparator'))
        select @MasterPath=substring(physical_name, 1, len(physical_name) - charindex(@Slash, reverse(physical_name))) from master.sys.database_files where name=N'master'
        select @LogPath=substring(physical_name, 1, len(physical_name) - charindex(@Slash, reverse(physical_name))) from master.sys.database_files where name=N'mastlog'
        select @ErrorLog=cast(SERVERPROPERTY(N'errorlogfilename') as nvarchar(512))
        select @ErrorLogPath=IIF(@ErrorLog IS NULL, N'', substring(@ErrorLog, 1, len(@ErrorLog) - charindex(@Slash, reverse(@ErrorLog))))
      


        declare @SmoRoot nvarchar(512)
        exec master.dbo.xp_instance_regread @HkeyLocal, @SetupRegPath, N'SQLPath', @SmoRoot OUTPUT
      


        declare @ServiceStartMode int
        EXEC master.sys.xp_instance_regread @HkeyLocal, @SqlServiceRegPath, N'Start', @ServiceStartMode OUTPUT
      


        declare @ServiceAccount nvarchar(512)
        EXEC master.sys.xp_instance_regread @HkeyLocal, @SqlServiceRegPath, N'ObjectName', @ServiceAccount OUTPUT
      


        declare @NamedPipesEnabled int
        exec master.dbo.xp_instance_regread @HkeyLocal, @NpRegPath, N'Enabled', @NamedPipesEnabled OUTPUT
      


        declare @TcpEnabled int
        EXEC master.sys.xp_instance_regread @HkeyLocal, @TcpRegPath, N'Enabled', @TcpEnabled OUTPUT
      


        declare @InstallSharedDirectory nvarchar(512)
        EXEC master.sys.xp_instance_regread @HkeyLocal, @SetupRegPath, N'SQLPath', @InstallSharedDirectory OUTPUT
      


        declare @SqlGroup nvarchar(512)
        exec master.dbo.xp_instance_regread @HkeyLocal, @SetupRegPath, N'SQLGroup', @SqlGroup OUTPUT
      


        declare @FilestreamLevel int
        exec master.dbo.xp_instance_regread @HkeyLocal, @FilestreamRegPath, N'EnableLevel', @FilestreamLevel OUTPUT
      


        declare @FilestreamShareName nvarchar(512)
        exec master.dbo.xp_instance_regread @HkeyLocal, @FilestreamRegPath, N'ShareName', @FilestreamShareName OUTPUT
      


        declare @cluster_name nvarchar(128)
        declare @quorum_type tinyint
        declare @quorum_state tinyint
        BEGIN TRY
        SELECT @cluster_name = cluster_name,
        @quorum_type = quorum_type,
        @quorum_state = quorum_state
        FROM sys.dm_hadr_cluster
        END TRY
        BEGIN CATCH
        --Querying this DMV using a contained auth connection throws error 15562 (Module is untrusted)
        --because of lack of trustworthiness by the server. This is expected so we just leave the
        --values as default
        IF(ERROR_NUMBER() NOT IN (297,300, 15562))
        BEGIN
        THROW
        END
        END CATCH
      

SELECT
@SmoAuditLevel AS [AuditLevel],
ISNULL(@NumErrorLogs, -1) AS [NumberOfLogFiles],
(case when @SmoLoginMode < 3 then @SmoLoginMode else 9 end) AS [LoginMode],
ISNULL(@SmoMailProfile,N'') AS [MailProfile],
@BackupDirectory AS [BackupDirectory],
@SmoPerfMonMode AS [PerfMonMode],
ISNULL(@InstallSqlDataDir,N'') AS [InstallDataDirectory],
CAST(@@SERVICENAME AS sysname) AS [ServiceName],
@ErrorLogPath AS [ErrorLogPath],
@SmoRoot AS [RootDirectory],
CAST(case when 'a' <> 'A' then 1 else 0 end AS bit) AS [IsCaseSensitive],
@@MAX_PRECISION AS [MaxPrecision],
CAST(FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') AS bit) AS [IsFullTextInstalled],
SERVERPROPERTY(N'ProductVersion') AS [VersionString],
CAST(SERVERPROPERTY(N'Edition') AS sysname) AS [Edition],
CAST(SERVERPROPERTY(N'ProductLevel') AS sysname) AS [ProductLevel],
CAST(SERVERPROPERTY('IsSingleUser') AS bit) AS [IsSingleUser],
CAST(SERVERPROPERTY('EngineEdition') AS int) AS [EngineEdition],
convert(sysname, serverproperty(N'collation')) AS [Collation],
CAST(ISNULL(SERVERPROPERTY('IsClustered'), 0) AS bit) AS [IsClustered],
CAST(ISNULL(SERVERPROPERTY(N'MachineName'), N'') AS sysname) AS [NetName],
ISNULL(SERVERPROPERTY(N'ComputerNamePhysicalNetBIOS'),N'') AS [ComputerNamePhysicalNetBIOS],
ISNULL(@ServiceStartMode,2) AS [ServiceStartMode],
@LogPath AS [MasterDBLogPath],
@MasterPath AS [MasterDBPath],
SERVERPROPERTY('instancedefaultdatapath') AS [DefaultFile],
SERVERPROPERTY('instancedefaultlogpath') AS [DefaultLog],
SERVERPROPERTY(N'ResourceVersion') AS [ResourceVersionString],
SERVERPROPERTY(N'ResourceLastUpdateDateTime') AS [ResourceLastUpdateDateTime],
SERVERPROPERTY(N'CollationID') AS [CollationID],
SERVERPROPERTY(N'ComparisonStyle') AS [ComparisonStyle],
SERVERPROPERTY(N'SqlCharSet') AS [SqlCharSet],
SERVERPROPERTY(N'SqlCharSetName') AS [SqlCharSetName],
SERVERPROPERTY(N'SqlSortOrder') AS [SqlSortOrder],
SERVERPROPERTY(N'SqlSortOrderName') AS [SqlSortOrderName],
SERVERPROPERTY(N'BuildClrVersion') AS [BuildClrVersionString],
ISNULL(@ServiceAccount,N'') AS [ServiceAccount],
CAST(@NamedPipesEnabled AS bit) AS [NamedPipesEnabled],
CAST(@TcpEnabled AS bit) AS [TcpEnabled],
ISNULL(@InstallSharedDirectory,N'') AS [InstallSharedDirectory],
ISNULL(suser_sname(sid_binary(ISNULL(@SqlGroup,N''))),N'') AS [SqlDomainGroup],
case when 1=msdb.dbo.fn_syspolicy_is_automation_enabled() and exists (select * from msdb.dbo.syspolicy_system_health_state  where target_query_expression_with_id like 'Server%' ) then 1 else 0 end AS [PolicyHealthState],
@FilestreamLevel AS [FilestreamLevel],
ISNULL(@FilestreamShareName,N'') AS [FilestreamShareName],
-1 AS [TapeLoadWaitTime],
CAST(SERVERPROPERTY(N'IsHadrEnabled') AS bit) AS [IsHadrEnabled],
SERVERPROPERTY(N'HADRManagerStatus') AS [HadrManagerStatus],
ISNULL(@cluster_name, '') AS [ClusterName],
ISNULL(@quorum_type, 4) AS [ClusterQuorumType],
ISNULL(@quorum_state, 3) AS [ClusterQuorumState],
SUSER_SID(@ServiceAccount, 0) AS [ServiceAccountSid],
CAST(SERVERPROPERTY('IsPolyBaseInstalled') AS bit) AS [IsPolyBaseInstalled],
CAST(
        serverproperty(N'Servername')
       AS sysname) AS [Name],
CAST(
        ISNULL(serverproperty(N'instancename'),N'')
       AS sysname) AS [InstanceName],
CAST(0x0001 AS int) AS [Status],
SERVERPROPERTY('PathSeparator') AS [PathSeparator],
0 AS [IsContainedAuthentication],
CAST(null AS int) AS [ServerType]
go

select * into #tmpdbr_database_replica_states from master.sys.dm_hadr_database_replica_states
select agstates.group_id, agstates.primary_replica into #tmpag_availability_group_states from master.sys.dm_hadr_availability_group_states as agstates
SELECT * into #tmpag_availability_groups from master.sys.availability_groups
select group_id, replica_id, replica_metadata_id into #tmpag_availability_replicas from master.sys.availability_replicas
select replica_id, is_local, role into #tmpag_availability_replica_states from master.sys.dm_hadr_availability_replica_states
--select agstates.group_id, agstates.primary_replica into #tmpag_availability_group_states from master.sys.dm_hadr_availability_group_states as agstates
select replica_id,role,is_local into #tmpdbr_availability_replica_states from master.sys.dm_hadr_availability_replica_states
select group_id, replica_id,replica_server_name,availability_mode into #tmpdbr_availability_replicas from master.sys.availability_replicas where availability_mode <> 4
select replica_id,group_database_id,database_name,is_database_joined,is_failover_ready into #tmpdbr_database_replica_cluster_states from master.sys.dm_hadr_database_replica_cluster_states
SELECT
AG.name AS [Name],
ISNULL(AG.automated_backup_preference, 4) AS [AutomatedBackupPreference],
ISNULL(AG.failure_condition_level, 6) AS [FailureConditionLevel],
ISNULL(AG.health_check_timeout, -1) AS [HealthCheckTimeout],
AR2.replica_metadata_id AS [ID],
ISNULL(arstates2.role, 3) AS [LocalReplicaRole],
ISNULL(agstates.primary_replica, '') AS [PrimaryReplicaServerName],
AG.group_id AS [UniqueId],
CAST(ISNULL(AG.basic_features, 0) AS bit) AS [BasicAvailabilityGroup],
CAST(ISNULL(AG.db_failover, 0) AS bit) AS [DatabaseHealthTrigger],
CAST(ISNULL(AG.dtc_support, 0) AS bit) AS [DtcSupportEnabled],
CAST(ISNULL(AG.is_distributed, 0) AS bit) AS [IsDistributedAvailabilityGroup],
ISNULL(AG.cluster_type, 0) AS [ClusterType],
ISNULL(AG.required_synchronized_secondaries_to_commit, 0) AS [RequiredSynchronizedSecondariesToCommit]
FROM
#tmpag_availability_groups AS AG
LEFT OUTER JOIN #tmpag_availability_group_states as agstates ON AG.group_id = agstates.group_id
INNER JOIN #tmpag_availability_replicas AS AR2 ON AG.group_id = AR2.group_id
INNER JOIN #tmpag_availability_replica_states AS arstates2 ON AR2.replica_id = arstates2.replica_id AND arstates2.is_local = 1
--WHERE
--(AG.name=@_msparam_0)

SELECT ars.role, drs.database_id, drs.replica_id, drs.last_commit_time into #tmpdbr_database_replica_states_primary_LCT from  #tmpdbr_database_replica_states as drs left join #tmpdbr_availability_replica_states ars on drs.replica_id = ars.replica_id where ars.role = 1

SELECT
AR.replica_server_name AS [AvailabilityReplicaServerName],
dbcs.database_name AS [AvailabilityDatabaseName],
--dbcs.group_database_id AS [AvailabilityDateabaseId],
--AR.group_id AS [AvailabilityGroupId],
AG.name AS [AvailabilityGroupName],
--AR.replica_id AS [AvailabilityReplicaId],
ISNULL(dbr.database_id, 0) AS [DatabaseId],
ISNULL(dbr.end_of_log_lsn, 0) AS [EndOfLogLSN],
CASE dbcs.is_failover_ready WHEN 1 THEN 0 ELSE ISNULL(DATEDIFF(ss, dbr.last_commit_time, dbrp.last_commit_time), 0) END  AS [EstimatedDataLoss],
ISNULL(CASE dbr.redo_rate WHEN 0 THEN -1 ELSE CAST(dbr.redo_queue_size AS float) / dbr.redo_rate END, -1) AS [EstimatedRecoveryTime],
ISNULL(dbr.filestream_send_rate, -1) AS [FileStreamSendRate],
ISNULL(dbcs.is_failover_ready, 0) AS [IsFailoverReady],
ISNULL(dbcs.is_database_joined, 0) AS [IsJoined],
arstates.is_local AS [IsLocal],
ISNULL(dbr.is_suspended, 0) AS [IsSuspended],
ISNULL(dbr.last_commit_lsn, 0) AS [LastCommitLSN],
ISNULL(dbr.last_commit_time, 0) AS [LastCommitTime],
ISNULL(dbr.last_hardened_lsn, 0) AS [LastHardenedLSN],
ISNULL(dbr.last_hardened_time, 0) AS [LastHardenedTime],
ISNULL(dbr.last_received_lsn, 0) AS [LastReceivedLSN],
ISNULL(dbr.last_received_time, 0) AS [LastReceivedTime],
ISNULL(dbr.last_redone_lsn, 0) AS [LastRedoneLSN],
ISNULL(dbr.last_redone_time, 0) AS [LastRedoneTime],
ISNULL(dbr.last_sent_lsn, 0) AS [LastSentLSN],
ISNULL(dbr.last_sent_time, 0) AS [LastSentTime],
ISNULL(dbr.log_send_queue_size, -1) AS [LogSendQueueSize],
ISNULL(dbr.log_send_rate, -1) AS [LogSendRate],
ISNULL(dbr.recovery_lsn, 0) AS [RecoveryLSN],
ISNULL(dbr.redo_queue_size, -1) AS [RedoQueueSize],
ISNULL(dbr.redo_rate, -1) AS [RedoRate],
ISNULL(AR.availability_mode, 2) AS [ReplicaAvailabilityMode],
ISNULL(arstates.role, 3) AS [ReplicaRole],
ISNULL(dbr.suspend_reason, 7) AS [SuspendReason],
ISNULL(CASE dbr.log_send_rate WHEN 0 THEN -1 ELSE CAST(dbr.log_send_queue_size AS float) / dbr.log_send_rate END, -1) AS [SynchronizationPerformance],
ISNULL(dbr.synchronization_state, 0) AS [SynchronizationState],
ISNULL(dbr.truncation_lsn, 0) AS [TruncationLSN]
FROM
#tmpag_availability_groups AS AG
INNER JOIN #tmpdbr_availability_replicas AS AR ON AR.group_id=AG.group_id
INNER JOIN #tmpdbr_database_replica_cluster_states AS dbcs ON dbcs.replica_id = AR.replica_id
LEFT OUTER JOIN #tmpdbr_database_replica_states AS dbr ON dbcs.replica_id = dbr.replica_id AND dbcs.group_database_id = dbr.group_database_id
LEFT OUTER JOIN #tmpdbr_database_replica_states_primary_LCT AS dbrp ON dbr.database_id = dbrp.database_id
INNER JOIN #tmpdbr_availability_replica_states AS arstates ON arstates.replica_id = AR.replica_id
--WHERE
--(AG.name=@_msparam_0)
ORDER BY
[AvailabilityReplicaServerName] ASC,[AvailabilityDatabaseName] ASC


--select * from #tmpag_availability_groups-- AS AG
--select * from #tmpdbr_availability_replicas-- AS AR ON AR.group_id=AG.group_id
--select * from #tmpdbr_database_replica_cluster_states --AS dbcs ON dbcs.replica_id = AR.replica_id
select TOP 2 db.database_name,st.synchronization_state_desc,st.synchronization_health_desc, st.last_redone_time, DATEDIFF(mi,last_redone_time,GETDATE()) AS [#minutes delay],CONVERT(varchar(6) , (DATEDIFF(mi,last_redone_time,GETDATE()))/60)+ ':' + RIGHT('0' + CONVERT(varchar(2), ((DATEDIFF(mi,last_redone_time,GETDATE())) % 60) ), 2) AS [#time delay],xd.[cntr_value] as [%LogUsed]
from #tmpdbr_database_replica_states st
inner join #tmpdbr_database_replica_cluster_states db on db.replica_id=st.replica_id and db.group_database_id=st.group_database_id
 LEFT JOIN (          
  select object_name,counter_name,instance_name,cntr_value from sys.dm_os_performance_counters          
  where object_name like '%Databases%'          
  and counter_name in ('Percent Log Used')           
 ) XD on XD.instance_name =db.database_name
WHERE last_redone_time IS NOT NULL ORDER BY last_redone_time asc--AS dbr ON dbcs.replica_id = dbr.replica_id AND dbcs.group_database_id = dbr.group_database_id
--select * from #tmpdbr_database_replica_states_primary_LCT-- AS dbrp ON dbr.database_id = dbrp.database_id
--select * from #tmpdbr_availability_replica_states