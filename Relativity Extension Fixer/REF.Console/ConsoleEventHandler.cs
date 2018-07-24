﻿using System;
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
        public override kCura.EventHandler.Console GetConsole(kCura.EventHandler.ConsoleEventHandler.PageEvent pageEvent)
        {
            kCura.EventHandler.Console returnConsole = new kCura.EventHandler.Console();
            return returnConsole;
        }

        public override void OnButtonClick(kCura.EventHandler.ConsoleButton consoleButton)
        {
            switch (consoleButton.Name)
            {
                //Handle each Button's functionality
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
                return retVal;
            }
        }

        public static void RunSavedSearch(IRSAPIClient proxy, int artifactId)
        {
            // proxy.APIOptions.WorkspaceID is already set
            Query<Document> query = new Query<Document>
            {
                Condition = new SavedSearchCondition(artifactId),
                Fields = kCura.Relativity.Client.DTOs.FieldValue.SelectedFields
            };

            ResultSet<Document> docResults = proxy.Repositories.Document.Query(query);
        }
    }
}
