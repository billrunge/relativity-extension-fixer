using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using kCura.EventHandler;
using kCura.Relativity.Client;
using kCura.Relativity.Client.DTOs;
using Relativity.API;

namespace REF.Console
{
    [kCura.EventHandler.CustomAttributes.Description("Console EventHandler")]
    [System.Runtime.InteropServices.Guid("67bf919b-b566-4204-813c-ac4785ed17ef")]
    public class ConsoleEventHandler : kCura.EventHandler.ConsoleEventHandler
    {
        private const String CONSOLE_TITLE = "Extension Fixer";
        private const String INSERT_JOB_BUTTON_NAME = "_insertJobButton";
        private const String INSERT_JOB_DISPLAY_TEXT = "Start Job";
        private const String INSERT_JOB_TOOL_TIP = "Begin Extension Fixer Job";
        private const String APPLICATION_GUID = "250a9e9a-ed2e-43b7-8467-06d7b41a77c1";
        public static readonly Guid SEARCH_FIELD_GUID = new Guid("A36C14BE-621C-442C-8A0B-4450DB1391E9");

        private const String JOB_EXISTS_QUERY = "SELECT COUNT(*) FROM [ExtensionFixerQueue] WHERE [WorkspaceArtifactID] = @WorkspaceArtifactID AND [JobArtifactID] = @JobArtifactID";
        //You want to ensure that there is only one job per patient at any given time.
        private const String INSERT_JOB_QUERY = @"
                IF NOT EXISTS(SELECT TOP 1 * FROM [ExtensionFixerQueue] WHERE [WorkspaceArtifactID] = @WorkspaceArtifactID AND [JobArtifactID] = @JobArtifactID)
                BEGIN
                INSERT INTO [ExtensionFixerQueue] (WorkspaceArtifactID, JobArtifactID, SourceSearchArtifactID, [Status], CreatedOn, LastModifiedOn)
                Values (@WorkspaceArtifactID, @JobArtifactID, 1042643, 0, GETUTCDATE(),GETUTCDATE())
                END";

        public override kCura.EventHandler.Console GetConsole(kCura.EventHandler.ConsoleEventHandler.PageEvent pageEvent)
        {
            //Construct a console object to build the console appearing in the UI.
            kCura.EventHandler.Console returnConsole = new kCura.EventHandler.Console();
            returnConsole.Items = new List<kCura.EventHandler.IConsoleItem>();
            returnConsole.Title = CONSOLE_TITLE;

            //Construct the submit job button.
            kCura.EventHandler.ConsoleButton submitJobButton = new kCura.EventHandler.ConsoleButton();
            submitJobButton.Name = INSERT_JOB_BUTTON_NAME;
            submitJobButton.DisplayText = INSERT_JOB_DISPLAY_TEXT;
            submitJobButton.ToolTip = INSERT_JOB_TOOL_TIP;
            submitJobButton.RaisesPostBack = true;

            //If a job is already in the queue, change the text and disable the button.
            if (pageEvent == PageEvent.PreRender)
            {
                System.Data.SqlClient.SqlParameter workspaceArtifactIDParam = new System.Data.SqlClient.SqlParameter("@WorkspaceArtifactID", System.Data.SqlDbType.Int);
                workspaceArtifactIDParam.Value = this.Helper.GetActiveCaseID();

                System.Data.SqlClient.SqlParameter jobArtifactIDParam = new System.Data.SqlClient.SqlParameter("@JobArtifactID", System.Data.SqlDbType.Int);
                jobArtifactIDParam.Value = this.ActiveArtifact.ArtifactID;

                int jobCount = this.Helper.GetDBContext(-1).ExecuteSqlStatementAsScalar<Int32>(JOB_EXISTS_QUERY, new System.Data.SqlClient.SqlParameter[] { workspaceArtifactIDParam, jobArtifactIDParam });

                //Use the helper function to check if a job currently exists. Set Enabled to the opposite value.
                if (jobCount > 0)
                {
                    submitJobButton.Enabled = false;
                }
                else
                {
                    submitJobButton.Enabled = true;
                }

            }

            //Add the buttons to the console.
            returnConsole.Items.Add(submitJobButton);

            return returnConsole;


        }

        public override void OnButtonClick(kCura.EventHandler.ConsoleButton consoleButton)
        {
            switch (consoleButton.Name)
            {
                case INSERT_JOB_BUTTON_NAME:
                    //The user clicked the button for the insert job so add the job to the queue table on the EDDS database.
                    System.Data.SqlClient.SqlParameter workspaceArtifactIDParam = new System.Data.SqlClient.SqlParameter("@WorkspaceArtifactID", System.Data.SqlDbType.Int);
                    workspaceArtifactIDParam.Value = this.Helper.GetActiveCaseID();
                    System.Data.SqlClient.SqlParameter jobArtifactIDParam = new System.Data.SqlClient.SqlParameter("@JobArtifactID", System.Data.SqlDbType.Int);
                    jobArtifactIDParam.Value = this.ActiveArtifact.ArtifactID;


                    //string searchArtifactID = this.ActiveArtifact.Fields.;

                    //searchArtifactID = this.ActiveArtifact.

                    this.Helper.GetDBContext(-1).ExecuteNonQuerySQLStatement(INSERT_JOB_QUERY, new System.Data.SqlClient.SqlParameter[] { workspaceArtifactIDParam, jobArtifactIDParam });

                    break;
            }
        }

        /// <summary>
        /// The RequiredFields property tells Relativity that your event handler needs to have access to specific fields that you return in this collection property
        /// regardless if they are on the current layout or not. These fields will be returned in the ActiveArtifact.Fields collection just like other fields that are on
        /// the current layout when the event handler is executed.
        /// </summary>
        public override kCura.EventHandler.FieldCollection RequiredFields
        {
            get
            {
                kCura.EventHandler.FieldCollection retVal = new kCura.EventHandler.FieldCollection();
                retVal.Add(new kCura.EventHandler.Field(SEARCH_FIELD_GUID));
                return retVal;
            }
        }

    }
}
