#Requires -Module @{ModuleName="VMware.VimAutomation.Core"; ModuleVersion="11.5.0.0"}

# Enable verbose output
$VerbosePreference = "Continue"

# Global variable for log file path
$global:logFile = $null

#region Utility Functions
function Write-DetailedLog {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "{0} - {1}: {2}" -f $timestamp, $Level, $Message
    Write-Verbose $logMessage
    if ($global:logFile) {
        try {
            Add-Content -Path $global:logFile -Value $logMessage
        }
        catch {
            Write-Host "Failed to write to log file: $_"
        }
    }
}

function Write-ImmediateOutput {
    param([string]$Message)
    Write-Host $Message
}

function Get-UniqueFilename {
    param (
        [string]$BaseFilename,
        [string]$Extension
    )
    $counter = 0
    $filename = "${BaseFilename}_${counter}.${Extension}"
    while (Test-Path $filename) {
        $counter++
        $filename = "${BaseFilename}_${counter}.${Extension}"
    }
    return $filename
}
#endregion

#region Data Retrieval Functions
function Get-BaselineData {
    param ([Parameter(Mandatory = $true)][string]$Path)

    Write-DetailedLog "Loading baseline data from $Path"

    $excel = $null
    $workbook = $null

    try {
        $excel = New-Object -ComObject Excel.Application
        $excel.Visible = $false
        $workbook = $excel.Workbooks.Open($Path)

        $baselineData = @{}

        foreach ($worksheet in $workbook.Worksheets) {
            Write-DetailedLog "Processing worksheet: $($worksheet.Name)"
            
            $usedRange = $worksheet.UsedRange
            $rowCount = $usedRange.Rows.Count
            $colCount = $usedRange.Columns.Count

            Write-DetailedLog "Worksheet dimensions: $rowCount rows, $colCount columns"

            # Read header row
            $headers = @()
            for ($col = 1; $col -le $colCount; $col++) {
                $headerValue = $usedRange.Cells.Item(1, $col).Text
                $headers += $headerValue
                Write-DetailedLog "Column $col header: '$headerValue'"
            }

            # Find indices for required columns
            $roleNameCol = 0
            $assignedUsersCol = 0
            $privilegesCol = 0

            for ($i = 0; $i -lt $headers.Count; $i++) {
                if ($headers[$i] -like "*Role*Name*") { $roleNameCol = $i + 1 }
                if ($headers[$i] -like "*Assigned*Users*") { $assignedUsersCol = $i + 1 }
                if ($headers[$i] -like "*Privileges*") { $privilegesCol = $i + 1 }
            }

            Write-DetailedLog "Column indices - RoleName: $roleNameCol, AssignedUsers: $assignedUsersCol, Privileges: $privilegesCol"

            if ($roleNameCol -eq 0 -or $assignedUsersCol -eq 0 -or $privilegesCol -eq 0) {
                throw "Unable to find all required columns. Please check your Excel file."
            }

            $roles = @()
            for ($row = 2; $row -le $rowCount; $row++) {
                $roleName = $usedRange.Cells.Item($row, $roleNameCol).Text
                if (![string]::IsNullOrWhiteSpace($roleName)) {
                    $assignedUsers = $usedRange.Cells.Item($row, $assignedUsersCol).Text -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
                    $privileges = $usedRange.Cells.Item($row, $privilegesCol).Text -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
                    
                    $roles += [pscustomobject]@{
                        RoleName = $roleName.Trim()
                        AssignedUsers = $assignedUsers
                        Privileges = $privileges
                    }
                }
            }

            $baselineData[$worksheet.Name] = $roles
            Write-DetailedLog "Processed $($roles.Count) roles in worksheet $($worksheet.Name)"
        }

        Write-DetailedLog "Baseline data loaded successfully. Found $($baselineData.Keys.Count) worksheets."
        return $baselineData
    }
    catch {
        Write-DetailedLog "Error reading Excel file: $_" -Level "ERROR"
        throw
    }
    finally {
        if ($workbook) {
            try {
                $workbook.Close($false)
                [System.Runtime.Interopservices.Marshal]::ReleaseComObject($workbook) | Out-Null
            }
            catch {
                Write-DetailedLog "Error closing workbook: $_" -Level "WARNING"
            }
        }
        if ($excel) {
            try {
                $excel.Quit()
                [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
            }
            catch {
                Write-DetailedLog "Error quitting Excel: $_" -Level "WARNING"
            }
        }
        [System.GC]::Collect()
        [System.GC]::WaitForPendingFinalizers()
    }
}

function Get-VcenterList {
    param ([Parameter(Mandatory = $true)][string]$Path)

    Write-DetailedLog "Loading vCenter list from $Path"
    $vCenters = Get-Content $Path -ErrorAction Stop | ForEach-Object {
        $parts = $_ -split ','
        if ($parts.Count -eq 2) {
            [PSCustomObject]@{
                Name = $parts[0].Trim()
                ShortName = $parts[1].Trim()
            }
        } else {
            Write-DetailedLog "Invalid vCenter entry: $_" -Level "WARNING"
            $null
        }
    }
    $validVCenters = $vCenters | Where-Object { $_ -ne $null }
    Write-DetailedLog "Loaded $($validVCenters.Count) valid vCenter entries"
    return $validVCenters
}

function Get-VcenterRolesAndPermissions {
    param ([Parameter(Mandatory = $true)][string]$VcenterName)

    Write-DetailedLog "Fetching roles and permissions from $VcenterName"
    $roles = Get-VIRole -Server $VcenterName
    $allPermissions = Get-VIPermission -Server $VcenterName

    if (-not $roles) {
        Write-DetailedLog "No roles found for $VcenterName" -Level "WARNING"
        return @{}
    }

    $rolesData = @{}
    foreach ($role in $roles) {
        $permissions = $allPermissions | Where-Object { $_.Role -eq $role.Name }
        $assignedUsers = $permissions | Select-Object -ExpandProperty Principal -Unique
        $rolesData[$role.Name] = @{
            AssignedUsers = $assignedUsers
            Privileges    = $role.PrivilegeList
        }
    }
    return $rolesData
}
#endregion

#region Comparison Functions
function Compare-Roles {
    param (
        [hashtable]$VcenterRoles, 
        [array]$BaselineRoles
    )

    Write-DetailedLog "Starting role comparison"
    $comparisonResults = @()

    # Check for new roles
    foreach ($roleName in $VcenterRoles.Keys) {
        if ($roleName -notin $BaselineRoles.RoleName) {
            $comparisonResults += [pscustomobject]@{
                RoleName = $roleName
                Status = "New"
                AssignedUsers = $VcenterRoles[$roleName].AssignedUsers
                Privileges = $VcenterRoles[$roleName].Privileges
            }
        }
    }

    # Check for removed roles
    foreach ($baselineRole in $BaselineRoles) {
        if ($baselineRole.RoleName -notin $VcenterRoles.Keys) {
            $comparisonResults += [pscustomobject]@{
                RoleName = $baselineRole.RoleName
                Status = "Removed"
                AssignedUsers = $baselineRole.AssignedUsers
                Privileges = $baselineRole.Privileges
            }
        }
    }

    Write-DetailedLog "Role comparison completed. Changes found: $($comparisonResults.Count)"
    return $comparisonResults
}

function Track-RoleChanges {
    param (
        [string]$RoleName,
        [object]$VcenterRole,
        [object]$BaselineRole
    )

    $changes = @{
        RoleName = $RoleName
        AssignedUsers = @{
            Missing = @()    # Users in baseline but not in vCenter
            Extra = @()      # Users in vCenter but not in baseline
        }
        Privileges = @{
            Missing = @()    # Privileges in baseline but not in vCenter
            Extra = @()      # Privileges in vCenter but not in baseline
        }
    }

    # Compare AssignedUsers
    if ($null -ne $VcenterRole.AssignedUsers -and $null -ne $BaselineRole.AssignedUsers) {
        $changes.AssignedUsers.Missing = $BaselineRole.AssignedUsers | Where-Object { $_ -notin $VcenterRole.AssignedUsers }
        $changes.AssignedUsers.Extra = $VcenterRole.AssignedUsers | Where-Object { $_ -notin $BaselineRole.AssignedUsers }
    }

    # Compare Privileges
    if ($null -ne $VcenterRole.Privileges -and $null -ne $BaselineRole.Privileges) {
        $changes.Privileges.Missing = $BaselineRole.Privileges | Where-Object { $_ -notin $VcenterRole.Privileges }
        $changes.Privileges.Extra = $VcenterRole.Privileges | Where-Object { $_ -notin $BaselineRole.Privileges }
    }

    # Return changes only if there are differences
    if ($changes.AssignedUsers.Missing.Count -gt 0 -or 
        $changes.AssignedUsers.Extra.Count -gt 0 -or 
        $changes.Privileges.Missing.Count -gt 0 -or 
        $changes.Privileges.Extra.Count -gt 0) {
        return $changes
    }
    return $null
}
#region HTML Report Generation Functions
function Generate-HtmlReportHeader {
    return @"
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>vCenter Role Comparison Report</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; max-width: 1200px; margin: 0 auto; padding: 20px; background-color: #f4f4f4; }
            h1 { color: #2c3e50; text-align: center; padding: 20px 0; background-color: #ecf0f1; border-radius: 5px; }
            h2 { color: #34495e; border-bottom: 2px solid #bdc3c7; padding-bottom: 10px; margin-top: 30px; }
            table { width: 100%; border-collapse: collapse; margin-bottom: 20px; background-color: white; box-shadow: 0 1px 3px rgba(0,0,0,0.2); }
            th, td { padding: 12px; border: 1px solid #ddd; text-align: left; vertical-align: top; }
            th { background-color: #3498db; color: white; font-weight: bold; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            .vcenter-section { margin-bottom: 40px; background-color: white; padding: 20px; border-radius: 5px; box-shadow: 0 1px 3px rgba(0,0,0,0.2); }
            .summary { background-color: #3498db; color: white; padding: 10px; border-radius: 5px; margin-bottom: 20px; }
            .summary h3 { margin: 0; }
            .summary ul { list-style-type: none; padding: 0; }
            .summary li { margin-bottom: 5px; }
            .role-changes { margin-top: 20px; padding: 10px; background-color: #f8f9fa; border-radius: 5px; }
            .role-change-details { margin: 10px 0; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }
            .missing { color: #dc3545; margin: 5px 0; }
            .extra { color: #28a745; margin: 5px 0; }
            .changes-title { color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 5px; margin-top: 20px; }
            .error { color: #dc3545; padding: 10px; background-color: #f8d7da; border-radius: 5px; margin: 10px 0; }
        </style>
    </head>
    <body>
        <h1>vCenter Role Comparison Report</h1>
        <p style="text-align: center;">Report generated on $(Get-Date)</p>
"@
}

function Generate-HtmlTableForVCenter {
    param (
        [string]$VCenterName,
        [array]$ComparisonResults,
        [int]$TotalRoleCount,
        [hashtable]$VcenterRoles,
        [array]$BaselineRoles
    )

    $newRoles = @($ComparisonResults | Where-Object { $_.Status -eq "New" }).Count
    $removedRoles = @($ComparisonResults | Where-Object { $_.Status -eq "Removed" }).Count

    $html = "<div class='vcenter-section'>"
    $html += "<h2>vCenter: $VCenterName</h2>"
    
    # Summary Section
    $html += "<div class='summary'>"
    $html += "<h3>Summary</h3>"
    $html += "<ul>"
    $html += "<li>Total Roles in vCenter: $TotalRoleCount</li>"
    $html += "<li>New Roles: $newRoles</li>"
    $html += "<li>Removed Roles: $removedRoles</li>"
    $html += "<li>Total Changes: $($ComparisonResults.Count)</li>"
    $html += "</ul>"
    $html += "</div>"

    # New and Removed Roles Section
    if ($ComparisonResults.Count -gt 0) {
        $html += "<h3 class='changes-title'>New and Removed Roles</h3>"
        $html += "<table>"
        $html += "<tr><th>Role Name</th><th>Status</th><th>Assigned Users</th><th>Privileges</th></tr>"

        foreach ($result in $ComparisonResults) {
            $html += "<tr>"
            $html += "<td>$($result.RoleName)</td>"
            $html += "<td>$($result.Status)</td>"
            $html += "<td>" + ($result.AssignedUsers -join "<br>") + "</td>"
            $html += "<td>" + ($result.Privileges -join "<br>") + "</td>"
            $html += "</tr>"
        }

        $html += "</table>"
    }

    # Detailed Changes Section for Existing Roles
    $html += "<h3 class='changes-title'>Detailed Changes in Existing Roles</h3>"
    $changeFound = $false
    
    foreach ($roleName in $VcenterRoles.Keys) {
        $baselineRole = $BaselineRoles | Where-Object { $_.RoleName -eq $roleName }
        if ($baselineRole) {
            $changes = Track-RoleChanges -RoleName $roleName -VcenterRole $VcenterRoles[$roleName] -BaselineRole $baselineRole
            if ($changes) {
                $changeFound = $true
                $html += "<div class='role-change-details'>"
                $html += "<h4>$roleName</h4>"
                
                if ($changes.AssignedUsers.Missing.Count -gt 0 -or $changes.AssignedUsers.Extra.Count -gt 0) {
                    $html += "<p><strong>AssignedUsers Changes:</strong></p>"
                    if ($changes.AssignedUsers.Missing.Count -gt 0) {
                        $html += "<p class='missing'>Missing Users: " + ($changes.AssignedUsers.Missing -join ", ") + "</p>"
                    }
                    if ($changes.AssignedUsers.Extra.Count -gt 0) {
                        $html += "<p class='extra'>Extra Users: " + ($changes.AssignedUsers.Extra -join ", ") + "</p>"
                    }
                }
                
                if ($changes.Privileges.Missing.Count -gt 0 -or $changes.Privileges.Extra.Count -gt 0) {
                    $html += "<p><strong>Privileges Changes:</strong></p>"
                    if ($changes.Privileges.Missing.Count -gt 0) {
                        $html += "<p class='missing'>Missing Privileges: " + ($changes.Privileges.Missing -join ", ") + "</p>"
                    }
                    if ($changes.Privileges.Extra.Count -gt 0) {
                        $html += "<p class='extra'>Extra Privileges: " + ($changes.Privileges.Extra -join ", ") + "</p>"
                    }
                }
                
                $html += "</div>"
            }
        }
    }

    if (-not $changeFound) {
        $html += "<p>No changes detected in existing roles.</p>"
    }

    $html += "</div>"
    return $html
}

function Generate-HtmlReportFooter {
    return @"
    </body>
    </html>
"@
}
#endregion

#region Main Function
function Compare-VcenterRoles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$BaselinePath,

        [Parameter(Mandatory=$true)]
        [string]$VcenterInputFile,

        [Parameter(Mandatory=$false)]
        [string]$OutputDir = (Join-Path (Get-Location).Path "Output"),

        [Parameter(Mandatory=$false)]
        [PSCredential]$Credential = (Get-Credential -Message "Enter vCenter credentials")
    )

    Write-DetailedLog "Function Compare-VcenterRoles started"
    Write-ImmediateOutput "Compare-VcenterRoles function started"

    $basePath = (Get-Location).Path
    $reportDateTime = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
    $baseHtmlFileName = Join-Path $OutputDir "RoleComparisonReport_$reportDateTime"
    $htmlReportPath = Get-UniqueFilename -BaseFilename $baseHtmlFileName -Extension "html"
    $baseLogFileName = Join-Path $OutputDir "script_log_$reportDateTime"
    $global:logFile = Get-UniqueFilename -BaseFilename $baseLogFileName -Extension "txt"

    if (!(Test-Path $OutputDir)) {
        New-Item -ItemType Directory -Path $OutputDir | Out-Null
        Write-DetailedLog "Created output directory: $OutputDir"
    }

    Write-DetailedLog "Script started" -Level "INFO"

    try {
        $baselineData = Get-BaselineData -Path $BaselinePath
        Write-DetailedLog "Baseline data loaded successfully"
    }
    catch {
        Write-DetailedLog "Failed to load baseline data. Error: $_" -Level "ERROR"
        return
    }

    try {
        $vCenters = Get-VcenterList -Path $VcenterInputFile
        Write-DetailedLog "vCenter list loaded successfully"
    }
    catch {
        Write-DetailedLog "Failed to load vCenter list. Error: $_" -Level "ERROR"
        return
    }

    $htmlContent = Generate-HtmlReportHeader
    $jsonReport = @()

    foreach ($vCenter in $vCenters) {
        Write-DetailedLog "Processing vCenter: $($vCenter.Name)"
        try {
            Connect-VIServer -Server $vCenter.Name -Credential $Credential -ErrorAction Stop
            Write-DetailedLog "Connected to vCenter: $($vCenter.Name)"

            $vcenterRoles = Get-VcenterRolesAndPermissions -VcenterName $vCenter.Name
            $baselineRoles = $baselineData[$vCenter.ShortName]

            if (-not $baselineRoles) {
                Write-DetailedLog "No baseline data found for $($vCenter.ShortName). Skipping comparison." -Level "WARNING"
                $htmlContent += "<p class='error'>No baseline data found for $($vCenter.ShortName). Skipping comparison.</p>"
                continue
            }

            $comparisonResults = Compare-Roles -VcenterRoles $vcenterRoles -BaselineRoles $baselineRoles
            $totalRoleCount = $vcenterRoles.Count
            
            $htmlContent += Generate-HtmlTableForVCenter `
                -VCenterName $vCenter.ShortName `
                -ComparisonResults $comparisonResults `
                -TotalRoleCount $totalRoleCount `
                -VcenterRoles $vcenterRoles `
                -BaselineRoles $baselineRoles

            $jsonOutput = @{
                VCenterName = $vCenter.ShortName
                TotalRoles = $totalRoleCount
                NewRoles = @($comparisonResults | Where-Object { $_.Status -eq "New" })
                RemovedRoles = @($comparisonResults | Where-Object { $_.Status -eq "Removed" })
                DetailedChanges = @{}
            }

            foreach ($roleName in $vcenterRoles.Keys) {
                $baselineRole = $baselineRoles | Where-Object { $_.RoleName -eq $roleName }
                if ($baselineRole) {
                    $changes = Track-RoleChanges -RoleName $roleName -VcenterRole $vcenterRoles[$roleName] -BaselineRole $baselineRole
                    if ($changes) {
                        $jsonOutput.DetailedChanges[$roleName] = $changes
                    }
                }
            }

            $jsonReport += $jsonOutput

            Disconnect-VIServer -Server $vCenter.Name -Confirm:$false
            Write-DetailedLog "Disconnected from vCenter: $($vCenter.Name)"
        }
        catch {
            Write-DetailedLog "Error processing $($vCenter.Name): $_" -Level "ERROR"
            $htmlContent += "<p class='error'>Error processing $($vCenter.Name): $_</p>"
        }
    }

    $htmlContent += Generate-HtmlReportFooter

    # Save HTML report
    try {
        $htmlContent | Out-File -FilePath $htmlReportPath -Encoding utf8 -ErrorAction Stop
        Write-DetailedLog "HTML report saved successfully to: $htmlReportPath"
    }
    catch {
        Write-DetailedLog "Error saving HTML report: $_" -Level "ERROR"
    }

    # Save JSON report
    try {
        $jsonReportPath = $htmlReportPath -replace '\.html$', '.json'
        $jsonReport | ConvertTo-Json -Depth 10 | Out-File -FilePath $jsonReportPath -Encoding utf8 -ErrorAction Stop
        Write-DetailedLog "JSON report saved successfully to: $jsonReportPath"
    }
    catch {
        Write-DetailedLog "Error saving JSON report: $_" -Level "ERROR"
    }

    Write-DetailedLog "Script completed" -Level "INFO"
    Write-DetailedLog "Script completed successfully. HTML report saved to: $htmlReportPath"
    Write-DetailedLog "Script completed successfully. JSON report saved to: $jsonReportPath"
    Write-Host "Script completed successfully. HTML report saved to: $htmlReportPath"
    Write-Host "Script completed successfully. JSON report saved to: $jsonReportPath"
}
#endregion

#region Script Execution
try {
    Write-ImmediateOutput "Script started"

    Write-ImmediateOutput "Checking required modules..."
    $requiredModules = @("VMware.VimAutomation.Core")
    foreach ($module in $requiredModules) {
        if (-not (Get-Module -ListAvailable -Name $module)) {
            throw "Required module not found: $module"
        }
    }
    Write-ImmediateOutput "Required modules found"

    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $baselinePath = Join-Path $scriptDir "baseline.xlsx"
    $vcenterInputFile = Join-Path $scriptDir "vCenters.txt"
    $outputDir = Join-Path $scriptDir "Output"

    Write-ImmediateOutput "Starting Compare-VcenterRoles function with the following parameters:"
    Write-ImmediateOutput "Baseline Path: $baselinePath"
    Write-ImmediateOutput "vCenter Input File: $vcenterInputFile"
    Write-ImmediateOutput "Output Directory: $outputDir"

    # Ensure the output directory exists
    if (!(Test-Path $outputDir)) {
        New-Item -ItemType Directory -Path $outputDir | Out-Null
        Write-ImmediateOutput "Created output directory: $outputDir"
    }

    # Call the function with these parameters
    Compare-VcenterRoles -BaselinePath $baselinePath -VcenterInputFile $vcenterInputFile -OutputDir $outputDir -Verbose

    Write-ImmediateOutput "Compare-VcenterRoles function call completed"
}
catch {
    Write-ImmediateOutput "An error occurred: $_"
    Write-DetailedLog "An error occurred: $_" -Level "ERROR"
}
finally {
    if ($global:logFile) {
        Write-ImmediateOutput "Log file location: $global:logFile"
    }
    Write-ImmediateOutput "Script execution ended"
}