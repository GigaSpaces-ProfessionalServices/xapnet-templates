xapnet-templates
================

This repository served as a dumping ground for code related to the improvement of XAP.NET. Here is the explaination of projects.

1. exported-templates: after exporting the various projects from within visual studio they were placed here.
1. src/gigaspaces.powershell: contains some scripts used to create nuget packages and the local nuget repository.
1. templates: contains the three template projects (basic, nhibernate, ef6)
1. wizard/RenameWizard updates the application names with variables fo when they are created from visual studio.


Build the powershell library, navigate to the output (../deploy), and from there you can execute either powerhsell script described in the powershell execution (e.g. script-name gs-verison JSHOMEDIR)
