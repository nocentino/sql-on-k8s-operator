//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

// agCRYAML is a 3-replica AG CR used in AG lifecycle tests.
// The secret mssql-ag-secret must be created before applying.
const agCRYAML = `
apiVersion: sql.mssql.microsoft.com/v1alpha1
kind: SQLServerAvailabilityGroup
metadata:
  name: mssql-ag
  namespace: default
spec:
  agName: AG1
  image: mcr.microsoft.com/mssql/server:2022-latest
  edition: Developer
  acceptEula: "Y"
  saPasswordSecretRef:
    name: mssql-ag-secret
    key: SA_PASSWORD
  replicas:
    - name: primary
      availabilityMode: SynchronousCommit
      failoverMode: Automatic
    - name: secondary-1
      availabilityMode: SynchronousCommit
      failoverMode: Automatic
      readableSecondary: true
    - name: secondary-2
      availabilityMode: AsynchronousCommit
      failoverMode: Manual
      readableSecondary: true
  endpointPort: 5022
  listener:
    name: mssql-ag-listener
    port: 1433
    serviceType: ClusterIP
  mssqlConf:
    memory.memorylimitmb: "2048"
  storage:
    accessModes: [ReadWriteOnce]
    dataVolumeSize: 20Gi
  resources:
    requests:
      cpu: "500m"
      memory: "2Gi"
    limits:
      cpu: "2"
      memory: "4Gi"
`

// ag2ReplicaCRYAML is a minimal 2-replica AG used for scale-down tests.
const ag2ReplicaCRYAML = `
apiVersion: sql.mssql.microsoft.com/v1alpha1
kind: SQLServerAvailabilityGroup
metadata:
  name: mssql-ag
  namespace: default
spec:
  agName: AG1
  image: mcr.microsoft.com/mssql/server:2022-latest
  edition: Developer
  acceptEula: "Y"
  saPasswordSecretRef:
    name: mssql-ag-secret
    key: SA_PASSWORD
  replicas:
    - name: primary
      availabilityMode: SynchronousCommit
      failoverMode: Automatic
    - name: secondary-1
      availabilityMode: SynchronousCommit
      failoverMode: Automatic
      readableSecondary: true
  endpointPort: 5022
  listener:
    name: mssql-ag-listener
    port: 1433
    serviceType: ClusterIP
  mssqlConf:
    memory.memorylimitmb: "2048"
  storage:
    accessModes: [ReadWriteOnce]
    dataVolumeSize: 20Gi
  resources:
    requests:
      cpu: "500m"
      memory: "2Gi"
    limits:
      cpu: "2"
      memory: "4Gi"
`

// instanceCRYAML is a standalone SQLServerInstance used in standalone tests.
const instanceCRYAML = `
apiVersion: sql.mssql.microsoft.com/v1alpha1
kind: SQLServerInstance
metadata:
  name: mssql-e2e
  namespace: default
spec:
  image: mcr.microsoft.com/mssql/server:2022-latest
  edition: Developer
  acceptEula: "Y"
  saPasswordSecretRef:
    name: mssql-secret
    key: SA_PASSWORD
  mssqlConf:
    memory.memorylimitmb: "2048"
  storage:
    accessModes: [ReadWriteOnce]
    dataVolumeSize: 10Gi
  resources:
    requests:
      cpu: "500m"
      memory: "2Gi"
    limits:
      cpu: "2"
      memory: "4Gi"
`

// createTestDB is T-SQL that creates a test database and table for data-loss validation.
const createTestDB = `
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'testdb')
  CREATE DATABASE testdb;
`

const createTestTable = `
IF NOT EXISTS (SELECT 1 FROM testdb.sys.tables WHERE name = 't')
BEGIN
  CREATE TABLE testdb.dbo.t (
    id   INT IDENTITY(1,1) PRIMARY KEY,
    val  NVARCHAR(64)      NOT NULL,
    ts   DATETIME2         NOT NULL DEFAULT SYSDATETIME()
  );
END
`

// addDBToAG adds testdb to the AG (run on primary after backup is taken).
const addDBtoAGSQL = `
BACKUP DATABASE testdb TO DISK = '/var/opt/mssql/data/testdb.bak' WITH INIT, FORMAT;
ALTER AVAILABILITY GROUP AG1 ADD DATABASE testdb;
`

// joinDBOnSecondary restores and joins testdb on a secondary pod.
const joinDBOnSecondarySQL = `
RESTORE DATABASE testdb
  FROM DISK = '/var/opt/mssql/data/testdb.bak'
  WITH NORECOVERY, REPLACE;
ALTER DATABASE testdb SET HADR AVAILABILITY GROUP = AG1;
`

// removeDBFromAGSQL removes testdb from the AG gracefully (run on primary).
const removeDBFromAGSQL = `ALTER AVAILABILITY GROUP AG1 REMOVE DATABASE testdb;`
