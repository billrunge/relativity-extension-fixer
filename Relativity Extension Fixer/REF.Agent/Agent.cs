using kCura.Relativity.Client;
using kCura.Relativity.Client.DTOs;
using Relativity.API;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace ExtensionFixerAgent
{
    [kCura.Agent.CustomAttributes.Name("Relativity Extension Fixer Agent")]
    [System.Runtime.InteropServices.Guid("9344f2c0-1b87-4e49-8c25-9114209a7ce8")]
    public class RelativityAgent : kCura.Agent.AgentBase
    {
        public override void Execute()
        {
            //Get the current Agent artifactID
            Int32 agentArtifactID = this.AgentID;
            //Get a dbContext for the EDDS database
            Relativity.API.IDBContext eddsDBContext = this.Helper.GetDBContext(-1);

            try
            {
                //Check to see if there are any jobs in the queue that aren't assigned to a different agent
                Dictionary<string, int> jobInformation = GetJob(eddsDBContext, agentArtifactID);

                //If GetJob returns nothing raise a message saying that
                if (jobInformation is null)
                {
                    this.RaiseMessage("Queue is empty. Waiting for work.", 10);
                }
                //Otherwise get to work
                else
                {
                    //Set workspace and job artifactID from job dictionary
                    int workspaceArtifactID = jobInformation["WorkspaceArtifactID"];
                    int jobArtifactID = jobInformation["JobArtifactID"];
                    int sourceSearchArtifactID = jobInformation["SourceSearchArtifactID"];

                    //Get the workspace's DB context
                    Relativity.API.IDBContext workspaceDBContext = this.Helper.GetDBContext(workspaceArtifactID);

                    using (kCura.Relativity.Client.IRSAPIClient proxy = this.Helper.GetServicesManager().CreateProxy<kCura.Relativity.Client.IRSAPIClient>(Relativity.API.ExecutionIdentity.System))
                    {
                        proxy.APIOptions.WorkspaceID = workspaceArtifactID;
                        List<int> searchResults = RunSourceSearch(proxy, sourceSearchArtifactID);
                        CreatePopTable(workspaceDBContext, jobArtifactID, searchResults, true);
                    }

                    DataTable batch = GetJobBatch(workspaceDBContext, jobArtifactID, 5000);

                    if (batch is null)
                    {
                        UpdateJobStatus(eddsDBContext, agentArtifactID, workspaceArtifactID, jobArtifactID, 4);
                        CleanQueueTable(eddsDBContext);
                    }
                    else
                    {
                        UpdateJobStatus(eddsDBContext, agentArtifactID, workspaceArtifactID, jobArtifactID, 2);
                        DataTable updatedBatch = UpdateFilenameExtension(batch);
                        UpdateJobTable(workspaceDBContext, updatedBatch, jobArtifactID);
                        UpdateFileTable(workspaceDBContext, jobArtifactID);
                    }
                }


            }
            catch (System.Exception ex)
            {
                //Your Agent caught an exception
                this.RaiseError(ex.Message, ex.Message);
            }
        }

        /**
		 * Returns the name of agent
		 */
        public override string Name
        {
            get
            {
                return "Relativity Extension Fixer Agent";
            }
        }

        /**
         * Check header and return whether file is tif or jpg 
         * If file is neither return empty string so nothing gets appended
        */
        public static string GetImageType(string path)
        {
            string headerCode = GetHeaderInfo(path).ToUpper();

            if (headerCode.StartsWith("FFD8FFE0"))
            {
                return "jpg";
            }
            else if (headerCode.StartsWith("49492A"))
            {
                return "tif";
            }
            else
            {
                return ""; //UnKnown
            }
        }

        /**
        * Generate a string with the file header info
        */
        public static string GetHeaderInfo(string path)
        {
            byte[] buffer = new byte[8];

            BinaryReader reader = new BinaryReader(new FileStream(path, FileMode.Open));
            reader.Read(buffer, 0, buffer.Length);
            reader.Close();

            StringBuilder sb = new StringBuilder();
            foreach (byte b in buffer)
                sb.Append(b.ToString("X2"));

            return sb.ToString();
        }

        /**
        * 
        */
        public static DataTable GetJobBatch(Relativity.API.IDBContext workspaceDBContext, Int32 jobArtifactID, Int32 batchSize)
        {
            //Create temp table #TempFileIDs with top (batchSize) records
            //Update table status using temp table
            //Select using the temp table
            //Delete Temp table
            string sql = String.Format(@"
                IF OBJECT_ID('tempdb..#TempFileIDs') IS NOT NULL DROP TABLE #TempFileIDs;
                
                SELECT TOP({0}) [FileID] INTO #TempFileIDs FROM [REF_POP_{1}] WHERE [Status] = 0 ORDER BY [FileID] ASC;

                UPDATE REF SET REF.[Status] = 1 FROM [EDDSDBO].[REF_POP_{1}] AS REF
                INNER JOIN #TempFileIDs AS TFI
                ON REF.FileID = TFI.FileID;

                SELECT REF.[FileID], REF.[Filename], REF.[Location], REF.[Status]
                FROM [EDDSDBO].[REF_POP_{1}] AS REF
                INNER JOIN #TempFileIDs AS TFI
                ON REF.FileID = TFI.FileID;

                IF OBJECT_ID('tempdb..#TempFileIDs') IS NOT NULL DROP TABLE #TempFileIDs;", batchSize.ToString(), jobArtifactID.ToString());

            DataTable jobBatch = workspaceDBContext.ExecuteSqlStatementAsDataTable(sql);

            if (jobBatch.Rows.Count > 0)
            {
                return jobBatch;
            }
            else
            {
                return null;
            }
        }


        /**
         * loop through every row in the current batch's data table
         * and update [filename] to [filename] + . + extension
         */
        public static DataTable UpdateFilenameExtension(DataTable currentBatch)
        {
            foreach (DataRow row in currentBatch.Rows)
            {
                string extension = GetImageType(row["Location"].ToString());
                row["Filename"] = row["Filename"] + "." + extension;
            }
            return currentBatch;
        }


        public static void CreatePopTable(Relativity.API.IDBContext workspaceDBContext, int jobArtifactID, List<int> searchArtifacts, bool imagesOnly)
        {
            string sql = String.Format(@"
                IF OBJECT_ID('[REF_POP_{0}]') IS NULL 

                CREATE TABLE [REF_POP_{0}](
	            [FileID] [int] NOT NULL,
                [Filename] [nvarchar](200) NOT NULL,
                [Location] [nvarchar](2000) NOT NULL,
	            [Status] [int] NOT NULL);", jobArtifactID.ToString());

            workspaceDBContext.ExecuteNonQuerySQLStatement(sql);

            foreach (int documentArtifactID in searchArtifacts)
            {
                if (imagesOnly)
                {
                    sql = String.Format(@"
                        INSERT INTO [REF_POP_{0}]
                        SELECT F.[FileID]
                        , F.[Filename]
                        , F.[Location]
                        , 0
                        FROM [File] AS F
                        LEFT JOIN [REF_POP_{0}] AS REF
                        ON REF.[FileID] = F.[FileID]
                        WHERE F.[Type] IN (1,3) 
                        AND F.[Filename] NOT LIKE '%.%'
                        AND REF.[FileID] IS NULL
                        AND F.[DocumentArtifactID] = {1}", jobArtifactID.ToString(), documentArtifactID.ToString());
                }
                else
                {
                    sql = String.Format(@"
                        INSERT INTO [REF_POP_{0}]
                        SELECT F.[FileID]
                        , F.[Filename]
                        , F.[Location]
                        , 0
                        FROM [File] AS F
                        LEFT JOIN [REF_POP_{0}] AS REF
                        ON REF.[FileID] = F.[FileID]
                        WHERE F.[Filename] NOT LIKE '%.%'
                        AND REF.[FileID] IS NULL
                        AND F.[DocumentArtifactID] = {1}", jobArtifactID.ToString(), documentArtifactID.ToString());
                }
                workspaceDBContext.ExecuteNonQuerySQLStatement(sql);
            }
        }

        public static void UpdateJobTable(Relativity.API.IDBContext workspaceDBContext, DataTable completedBatch, Int32 jobArtifactID)
        {
            foreach (DataRow row in completedBatch.Rows)
            {
                string sql = String.Format(@"
                      UPDATE [EDDSDBO].[REF_POP_{0}]
                      SET [Filename] = '{1}',
                      [Status] = 3
                      WHERE FileID = {2}", jobArtifactID.ToString(), row["Filename"].ToString(), row["FileID"].ToString());
                workspaceDBContext.ExecuteNonQuerySQLStatement(sql);
            }
        }

        public static void UpdateFileTable(Relativity.API.IDBContext workspaceDBContext, Int32 jobArtifactID)
        {
            string sql = String.Format(@"
                UPDATE F
                SET F.[Filename] = REF.[Filename]
                FROM [EDDSDBO].[File] AS F
                INNER JOIN [EDDSDBO].[REF_POP_{0}] AS REF
                ON F.[FileID] = REF.[FileID]
                WHERE [REF].[Status] = 3;

                UPDATE [EDDSDBO].[REF_POP_{0}]
                SET [Status] = 4
                WHERE [Status] = 3;

                DELETE FROM [EDDSDBO].[REF_POP_{0}]
                WHERE [Status] = 4;", jobArtifactID);

            workspaceDBContext.ExecuteNonQuerySQLStatement(sql);
        }

        public static Boolean DoesAgentHaveJob(Relativity.API.IDBContext eddsDBContext, Int32 agentArtifactID)
        {
            string sql = String.Format(@"
                SELECT COUNT(*) 
                FROM [EDDS].[eddsdbo].[ExtensionFixerQueue] 
                WHERE [AgentArtifactID] = {0}", agentArtifactID.ToString());
            Int32 jobCount = Int32.Parse(eddsDBContext.ExecuteSqlStatementAsScalar(sql).ToString());
            if (jobCount > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static Int32 JobQueueDepth(Relativity.API.IDBContext eddsDBContext)
        {
            string sql = @"
                SELECT COUNT(*) 
                FROM [EDDS].[eddsdbo].[ExtensionFixerQueue]";

            return Int32.Parse(eddsDBContext.ExecuteSqlStatementAsScalar(sql).ToString());

        }

        public static Dictionary<string, int> GetJob(Relativity.API.IDBContext eddsDBContext, Int32 agentArtifactID)
        {
            Int32 workspaceArtifactID;
            Int32 jobArtifactID;
            Int32 sourceSearchArtifactID;

            if (DoesAgentHaveJob(eddsDBContext, agentArtifactID))
            {
                string sql = String.Format(@"
                    SELECT TOP 1 [JobArtifactID], [WorkspaceArtifactID], [SourceSearchArtifactID]
                    FROM [EDDS].[eddsdbo].[ExtensionFixerQueue]
                    WHERE AgentArtifactID = {0}
                    AND [Status] NOT IN (4, 5)", agentArtifactID);

                DataTable jobInfo = eddsDBContext.ExecuteSqlStatementAsDataTable(sql);
                DataRow row = jobInfo.Rows[0];
                workspaceArtifactID = Int32.Parse(row["WorkspaceArtifactID"].ToString());
                jobArtifactID = Int32.Parse(row["JobArtifactID"].ToString());
                sourceSearchArtifactID = Int32.Parse(row["SourceSearchArtifactID"].ToString());

                Dictionary<string, int> outputDict = new Dictionary<string, int>
                {
                    { "WorkspaceArtifactID", workspaceArtifactID },
                    { "JobArtifactID", jobArtifactID },
                    { "SourceSearchArtifactID", sourceSearchArtifactID }
                };
                return outputDict;

            }
            else if (JobQueueDepth(eddsDBContext) > 0)
            {
                string sql = @"
                    SELECT TOP 1 [JobArtifactID], [WorkspaceArtifactID], [SourceSearchArtifactID]
                    FROM [EDDS].[eddsdbo].[ExtensionFixerQueue]
                    WHERE [Status] = 0";

                DataTable jobInfo = eddsDBContext.ExecuteSqlStatementAsDataTable(sql);
                DataRow row = jobInfo.Rows[0];
                workspaceArtifactID = Int32.Parse(row["WorkspaceArtifactID"].ToString());
                jobArtifactID = Int32.Parse(row["JobArtifactID"].ToString());
                sourceSearchArtifactID = Int32.Parse(row["SourceSearchArtifactID"].ToString());

                sql = String.Format(@"
                    UPDATE [EDDS].[eddsdbo].[ExtensionFixerQueue]
                    SET [Status] = 2, [AgentArtifactID] = {0}
                    WHERE [WorkspaceArtifactID] = {1} 
                    AND [JobArtifactID] = {2}", agentArtifactID, workspaceArtifactID, jobArtifactID);

                eddsDBContext.ExecuteNonQuerySQLStatement(sql);

                Dictionary<string, int> outputDict = new Dictionary<string, int>
                {
                    { "WorkspaceArtifactID", workspaceArtifactID },
                    { "JobArtifactID", jobArtifactID },
                    { "SourceSearchArtifactID", sourceSearchArtifactID }
                };

                return outputDict;
            }
            else
            {
                return null;
            }


        }

        public static void UpdateJobStatus(Relativity.API.IDBContext eddsDBContext, Int32 agentArtifactID, Int32 workspaceArtifactID, Int32 jobArtifactID, Int32 statusCode)
        {
            string sql = String.Format(@"                    
                    UPDATE [EDDS].[eddsdbo].[ExtensionFixerQueue]
                    SET [Status] = {0}
                    WHERE [WorkspaceArtifactID] = {1}
                    AND [JobArtifactID] = {2}
                    AND [AgentArtifactID] = {3}", statusCode, workspaceArtifactID, jobArtifactID, agentArtifactID);

            eddsDBContext.ExecuteNonQuerySQLStatement(sql);
        }

        public static void CleanQueueTable(Relativity.API.IDBContext eddsDBContext)
        {
            string sql = @"                    
                    DELETE FROM [EDDS].[eddsdbo].[ExtensionFixerQueue]
                    WHERE [Status] = 4";

            eddsDBContext.ExecuteNonQuerySQLStatement(sql);
        }


        public static List<int> RunSourceSearch(IRSAPIClient workspaceProxy, int searchArtifactID)
        {
            List<int> artifactList = new List<int>();

            //Set RSAPI to the appropriate workspace
            //proxy.APIOptions.WorkspaceID = workspaceArtifactID;

            //Create query object
            Query<Document> query = new Query<Document>
            {
                //Add data source saved search as a condition
                Condition = new SavedSearchCondition(searchArtifactID)
            };

            //Configure query to only return document ArtifactIDs
            query.Fields.Add(new FieldValue(ArtifactQueryFieldNames.ArtifactID));

            //Run the search and first 1000 results batch
            int batchSize = 1000;
            QueryResultSet<Document> masterSet = workspaceProxy.Repositories.Document.Query(query, batchSize);

            //Confirm that the search has ran successfully and returned results
            if (masterSet.Success)
            {
                if (masterSet.Results.Count > 0)
                {
                    //Get count of current batch
                    int counter = masterSet.Results.Count;

                    //Create query token in case there is more than 1,000 results
                    string queryToken = masterSet.QueryToken;

                    //Loop through each result and add to a list
                    foreach (Result<Document> result in masterSet.Results)
                    {
                        artifactList.Add(result.Artifact.ArtifactID);
                    }

                    //Get total count of documents returned by search
                    int totalDocs = masterSet.TotalCount;

                    //Create flag to indicate if more records returned than current counter
                    bool hasMoreDocumentsToRead = false;

                    //If the total count of docs is greater than the current counter, set flag
                    if (totalDocs > counter)
                    {
                        hasMoreDocumentsToRead = true;
                    }

                    while (hasMoreDocumentsToRead)
                    {
                        //Create a secondary set
                        QueryResultSet<Document> secondarySet = null;

                        //Run a subquery using the query token, the counter and batch size
                        secondarySet = workspaceProxy.Repositories.Document.QuerySubset(queryToken, counter + 1, batchSize);
                        if (secondarySet.Success)
                        {
                            if (secondarySet.Results.Count > 0)
                            {
                                //Update the counter
                                counter += secondarySet.Results.Count;

                                //Loop through each result and add to a list
                                foreach (Result<Document> result in secondarySet.Results)
                                {
                                    artifactList.Add(result.Artifact.ArtifactID);
                                }
                            }
                        }

                        //Set flag
                        if (totalDocs > counter)
                        {
                            hasMoreDocumentsToRead = true;
                        }
                        else
                        {
                            hasMoreDocumentsToRead = false;
                        }
                    }
                }





            }

            return artifactList;
        }

        public enum JobStatus
        {
            NotStarted = 0,
            Populating = 1,
            InProgress = 2,
            ReadyForImport = 3,
            Complete = 4,
            Error = 5
        }

        public class ExtensionFixerJob
        {
            public Int32 JobArtifactID { get; set; }
            public Int32 WorkspaceArtifactID { get; set; }
            // public JobStatus Status { get; set; }
        }


    }
}
