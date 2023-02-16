WITH avg_permit_time AS (
  SELECT
    [ahj],[state],
    AVG(COALESCE(
      [Median AHJ Permit Time 2017 0-10kW],
      [Median AHJ Permit Time 2018 0-10kW],
      [Median AHJ Permit Time 2019 0-10kW],
      [Median AHJ Permit Time 2020 0-10kW],
      [Median AHJ Permit Time 2021 0-10kW],
      [Median AHJ Permit Time 2017 11-50kW],
      [Median AHJ Permit Time 2018 11-50kW],
      [Median AHJ Permit Time 2019 11-50kW],
      [Median AHJ Permit Time 2020 11-50kW],
      [Median AHJ Permit Time 2021 11-50kW],
      0
    )
  ) AS avg_permit_time
  FROM [SolarData2]
  GROUP BY [ahj],[state]), --Group by AHJ and State are required for unique key later in the joins. There can be several AHJ's with same names. (i.e. Clark County NV & WI)

 avg_preinstall_time AS (
	Select
		[ahj],[state],
		avg(coalesce(
			[Median Pre-Install IX Time 2017 0-10kW],
			[Median Pre-Install IX Time 2017 11-50kW],
			[Median Pre-Install IX Time 2018 0-10kW],
			[Median Pre-Install IX Time 2018 11-50kW],
			[Median Pre-Install IX Time 2019 0-10kW],
			[Median Pre-Install IX Time 2019 11-50kW],
			[Median Pre-Install IX Time 2020 0-10kW],
			[Median Pre-Install IX Time 2020 11-50kW],
			[Median Pre-Install IX Time 2021 0-10kW],
			[Median Pre-Install IX Time 2021 11-50kW],
			0
		)
	) AS avg_preinstall_time
	from [SolarData2]
	GROUP BY [ahj],[state]),

	avg_inspection_time AS (
		Select
			[ahj],[state],
				avg(coalesce(
					[Median Inspection Time 2019 0-10kW],
					[Median Inspection Time 2019 11-50kW],
					[Median Inspection Time 2020 0-10kW],
					[Median Inspection Time 2020 11-50kW],
					[Median Inspection Time 2021 0-10kW],
					[Median Inspection Time 2021 11-50kW],
					[Median Inspection Time 2018 11-50kW],
					[Median Inspection Time 2018 0-10kW],
					[Median Inspection Time 2017 11-50kW],
					[Median Inspection Time 2017 0-10kW],
					0
				)
			)AS avg_inspection_time
			from [SolarData2]
			group by [ahj],[state]),

	avg_pto_time AS (
		Select
			[ahj],[state],
				avg(coalesce(
					[Median Final IX to PTO 2021 11-50kW],
					[Median Final IX to PTO 2021 0-10kW],
					[Median Final IX to PTO 2020 11-50kW],
					[Median Final IX to PTO 2020 0-10kW],
					[Median Final IX to PTO 2019 11-50kW],
					[Median Final IX to PTO 2019 0-10kW],
					[Median Final IX to PTO 2018 11-50kW],
					[Median Final IX to PTO 2018 0-10kW],
					[Median Final IX to PTO 2017 11-50kW],
					[Median Final IX to PTO 2017 0-10kW],
					0
				)
			)AS avg_pto_time
			from [SolarData2]
			group by [ahj],[state])

Select distinct
[SolarData2].[state],
SolarData2.ahj,
[SolarData2].geo_id,
[SolarData2].utility,
[SolarData2].[Total Installs],

round(avg_preinstall_time,2) as [AVG Preinstall Time],
100-ntile(100) over (order by avg_preinstall_time) PreinstallPCT, 
round(avg_permit_time,2) as [AVG Permit Time],
100-ntile(100) over (order by avg_permit_time) PermitPCT,
round(avg_inspection_time,2) as [AVG Inspection Time],
100-ntile(100) over (order by avg_inspection_time) InspectionPCT,
round(avg_pto_time,2) as [AVG PTO Time],
100-ntile(100) over (order by avg_pto_time) PTOPCT,
UTILITYData.[Response Time (Business days)] Interconnection,
100-ntile(100) over (order by UTILITYData.[Response Time (Business days)]) InterconnectionPCT,
round(avg_inspection_time+avg_permit_time+avg_preinstall_time+
avg_pto_time+UTILITYData.[Response Time (Business days)],1) Total,
100-ntile(100) over (order by round(avg_inspection_time+avg_permit_time+avg_preinstall_time+avg_pto_time+UTILITYData.[Response Time (Business days)],1)) TotalPCT,

UTILITYData.[Offset Policy in Place from Utiltiy] Offset_Policy,
UTILITYData.[Offset Percentage],
UTILITYData.[Level 1 Process Cutoff (KW)] KW_Limit,
UTILITYData.[Approval to Build required from utility before install] Utility_Approval,

case
	when AHJData.[Online Permitting]=1 THEN 'True'
	when AHJData.[Online Permitting]=0 THEN 'False'
	else 'N/A'
End as Online_Permitting,

AHJData.[Structural Review Required],
AHJData.[Median Permit Cost],
AHJData.[Number of Inspections Reqd]



from SolarData2
	join avg_permit_time
		on SolarData2.[ahj]+SolarData2.[state] = avg_permit_time.[ahj]+avg_permit_time.[state]
	join avg_inspection_time
		on SolarData2.[ahj]+SolarData2.[state] = avg_inspection_time.[ahj]+avg_inspection_time.[state]
	join avg_pto_time
		on SolarData2.[ahj]+SolarData2.[state] = avg_pto_time.[ahj]+avg_pto_time.[state]
	join avg_preinstall_time
		on SolarData2.[ahj]+SolarData2.[state] = avg_preinstall_time.[ahj]+avg_preinstall_time.[state]
	Full Outer join UTILITYData
		on SolarData2.eia_id = UTILITYData.[EIA ID]
	join AHJData
		on SolarData2.ahj+SolarData2.[state] = AHJData.ahj+AHJData.[state]

order by [SolarData2].[state], Total DESC













--NTILE(4)OVER (Partition by avg_preinstall_time order by SolarData2.utility) as Quartile,
