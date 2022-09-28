import os
from prediction_utils.extraction_utils.database import BQDatabase
from sepsis_labeler.component_base import Component

class SuspectedInfectionComponent(Component):
	'''
	Class to get suspected infections from cohort.
	'''
	def __init__(self, *args, **kwargs):
		Component.__init__(self, *args, **kwargs)
		
	def get_component_query(self, format_query=True):
		query = '''
				CREATE OR REPLACE TABLE `{suspected_infection}` AS
				{values_query}
				{window_query}
				{rollup_query} 
				select * from susp_inf_rollup
				'''
		if not format_query:
			pass
		else:
			query = query.format_map({**self.config_dict,**{"values_query":self.get_values_query(), "window_query":self.get_window_query(), "rollup_query":self.get_rollup_query()}})
			
		if self.config_dict['print_query']:
			print(query)
			
		return query

	def get_values_query(self, format_query=True):
		query = '''
				WITH 
				blood_culture_list AS (
					SELECT 
						descendant_concept_id AS concept_id
					FROM {dataset_project}.{dataset}.concept_ancestor
					WHERE ancestor_concept_id = 4107893
				),
				blood_culture_from_measurement_via_ancestor AS (  
					SELECT 
						person_id, 
						measurement_DATETIME
					FROM {dataset_project}.{dataset}.measurement AS measure
					WHERE measure.measurement_concept_id IN (
						SELECT concept_id
						FROM blood_culture_list)
				),
				systemic_abx_list AS (
					SELECT 
						descendant_concept_id AS concept_id
					FROM {dataset_project}.{dataset}.concept_ancestor
					WHERE ancestor_concept_id = 21602796 
				),
				systemic_abx_from_drug_exposure_via_ancestor AS (
					SELECT 
						person_id, 
						drug_concept_id, 
						drug_exposure_start_DATETIME
					FROM {dataset_project}.{dataset}.drug_exposure AS drug
					WHERE drug.drug_concept_id IN (
						SELECT concept_id
						FROM systemic_abx_list)
				),
				systemic_abx_from_drug_exposure_with_name AS (
					SELECT 
						systemic_abx.*, 
						concept.concept_name AS systemic_abx_type  
					FROM systemic_abx_from_drug_exposure_via_ancestor AS systemic_abx
					INNER JOIN {dataset_project}.{dataset}.concept AS concept
					ON systemic_abx.drug_concept_id = concept.concept_id
				),
				bc_abx AS (
					SELECT 
						blood_culture.person_id, 
						blood_culture.measurement_DATETIME as bc_DATETIME,
						systemic_abx.drug_exposure_start_DATETIME, 
						systemic_abx.systemic_abx_type
					FROM blood_culture_from_measurement_via_ancestor AS blood_culture
					LEFT JOIN systemic_abx_from_drug_exposure_with_name AS systemic_abx
					ON blood_culture.person_id = systemic_abx.person_id
				),
				admit_bc_abx AS (
					SELECT 
						admission_rollup.*, 
						bc_abx.bc_DATETIME, 
						bc_abx.drug_exposure_start_DATETIME, 
						bc_abx.systemic_abx_type   
					FROM `{admission_rollup}` as admission_rollup
					LEFT JOIN bc_abx AS bc_abx
					ON admission_rollup.person_id = bc_abx.person_id
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_window_query(self, format_query=True):
		query = '''
				susp_inf_window AS (
					SELECT 
						person_id, 
						bc_DATETIME, 
						drug_exposure_start_DATETIME,
						admit_date as admit_datetime,
						cast(admit_date as DATE) as admit_date,
						discharge_date as discharge_datetime,
						cast(discharge_date as DATE) as discharge_date, 
						systemic_abx_type,
						datetime_diff(drug_exposure_start_DATETIME, bc_DATETIME, DAY) as days_bc_abx
					FROM admit_bc_abx as admit_bc_abx
					WHERE
						CAST(bc_DATETIME AS DATE) >= DATE_SUB(admit_date, INTERVAL 1 DAY) AND  CAST(bc_DATETIME AS DATE) <= discharge_date AND
						CAST(drug_exposure_start_DATETIME AS DATE) >= DATE_SUB(admit_date, INTERVAL 1 DAY) AND  CAST(drug_exposure_start_DATETIME AS DATE) <= discharge_date
					AND
						CAST(bc_DATETIME AS DATE)<= CAST(DATETIME_ADD(drug_exposure_start_DATETIME, INTERVAL 1 DAY) AS DATE) AND
						CAST(bc_DATETIME AS DATE)>= CAST(DATETIME_SUB(drug_exposure_start_DATETIME, INTERVAL 3 DAY) AS DATE) 
					ORDER BY person_id, admit_date, bc_DATETIME, drug_exposure_start_DATETIME
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_rollup_query(self, format_query=True):
		query = '''
				susp_inf_rollup AS (
					SELECT 
					person_id, 
					admit_date,
					admit_datetime,
					MIN(discharge_date) AS discharge_date,
					MIN(discharge_datetime) as discharge_datetime,
					MIN(bc_DATETIME) as min_bc,
					MIN(drug_exposure_start_DATETIME) as min_systemic_abx,
					LEAST(MIN(bc_DATETIME),MIN(drug_exposure_start_DATETIME)) as index_date
				FROM susp_inf_window 
				GROUP BY person_id, admit_date, admit_datetime
				ORDER BY person_id, admit_date, admit_datetime
				)
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

class PlateletComponent(Component): 
	'''
	Class to get platelet count from cohort.
	'''
	def __init__(self, prior=False, *args, **kwargs):
		Component.__init__(self, *args, **kwargs)
		self.prior = prior
	
	def get_component_query(self, format_query=True):
		query = '''
				CREATE OR REPLACE TABLE `{sepsis_platelet}` AS
				{values_query}
				{window_query}
				{rollup_query} 
				select * from platelet_rollup
				'''
		if not format_query:
			pass
		else:
			query = query.format_map({**self.config_dict,**{"values_query":self.get_values_query(), "window_query":self.get_window_query(), "rollup_query":self.get_rollup_query()}})
			
		if self.config_dict['print_query']:
			print(query)
			
		return query

	def get_values_query(self, format_query=True):
		query = '''
				WITH platelet_from_measurement AS (
					SELECT 
						measure.person_id, 
						measure.measurement_DATETIME, 
						measure.value_as_number, 
						concept.concept_name AS measure_type  
					FROM {dataset_project}.{dataset}.measurement AS measure
					INNER JOIN {dataset_project}.{dataset}.concept AS concept
					ON measure.measurement_concept_id = concept.concept_id
					WHERE concept.concept_id in(
						SELECT
							c.concept_id
						FROM {dataset_project}.{dataset}.concept c
						WHERE c.concept_id in (37037425,40654106)
						UNION DISTINCT
						SELECT
							c.concept_id
						FROM {dataset_project}.{dataset}.concept c
						INNER JOIN {dataset_project}.{dataset}.concept_ancestor ca
						ON c.concept_id = ca.descendant_concept_id
						AND ca.ancestor_concept_id in (37037425,40654106)
						AND c.invalid_reason is null
					) AND measure.value_as_number IS NOT NULL AND measure.value_as_number > 0
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_window_query(self, format_query=True):
		query = '''
				platelet_window AS (
					SELECT 
						susp_inf_rollup.*, 
						platelet.measurement_DATETIME AS platelet_date, 
						platelet.value_as_number,
						datetime_diff(platelet.measurement_DATETIME, index_date, DAY) as days_plat_index
					FROM {suspected_infection} AS susp_inf_rollup
					LEFT JOIN platelet_from_measurement AS platelet USING (person_id)
					WHERE
						CAST(index_date AS DATE) >= CAST(DATETIME_SUB(measurement_DATETIME, INTERVAL 2 DAY) AS DATE) AND
						CAST(index_date AS DATE) <= CAST(DATETIME_ADD(measurement_DATETIME, INTERVAL 1 DAY) AS DATE) AND
						value_as_number IS NOT NULL
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_rollup_query(self, format_query=True):
		query = '''
				platelet_rollup AS (
					SELECT 
						person_id, 
						admit_date, 
						admit_datetime, 
						MIN(value_as_number) as min_platelet
					FROM platelet_window 
					GROUP BY person_id, admit_date, admit_datetime
				)
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

class CreatinineComponent(Component): 
	'''
	Class to get creatinine measurement from cohort.
	Units are normalized to mg/dL
	'''
	def __init__(self, prior=False, *args, **kwargs):
		Component.__init__(self, *args, **kwargs)
		self.prior = prior
	
	def get_component_query(self, format_query=True):
		query = '''
				CREATE OR REPLACE TABLE `{sepsis_creatinine}` AS
				{values_query}
				{window_query}
				{rollup_query} 
				select * from creatinine_rollup
				'''
		if not format_query:
			pass
		else:
			query = query.format_map({**self.config_dict,**{"values_query":self.get_values_query(), "window_query":self.get_window_query(), "rollup_query":self.get_rollup_query()}})
			
		if self.config_dict['print_query']:
			print(query)
			
		return query

	def get_values_query(self, format_query=True):
		query = '''
				WITH creatinine_from_measurement AS (
					SELECT 
						measure.person_id, 
						measure.measurement_DATETIME, 
						case
							when unit_concept_id = 8840 then value_as_number / 0.0113122 -- umol/l -> mg/dL
							when unit_concept_id = 8837 then value_as_number * 0.001 / 0.0113122 -- ug/dL -> mg/dL
							else value_as_number 
						end as value_as_number, 
						concept.concept_name AS measure_type  
					FROM {dataset_project}.{dataset}.measurement AS measure
					INNER JOIN {dataset_project}.{dataset}.concept AS concept
					ON measure.measurement_concept_id = concept.concept_id
					WHERE concept.concept_id in (
						SELECT
							c.concept_id
						FROM {dataset_project}.{dataset}.concept c
						WHERE c.concept_id in (37029387,4013964,2212294,3051825)
						UNION DISTINCT
						SELECT
							c.concept_id
						FROM {dataset_project}.{dataset}.concept c
						INNER JOIN {dataset_project}.{dataset}.concept_ancestor ca
						ON c.concept_id = ca.descendant_concept_id
						AND ca.ancestor_concept_id in (37029387,4013964,2212294,3051825)
						AND c.invalid_reason is null
					) AND measure.value_as_number IS NOT NULL AND measure.value_as_number > 0
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_window_query(self, format_query=True):
		query = '''
				creatinine_window AS (
					SELECT 
						susp_inf_rollup.*, 
						creatinine.measurement_DATETIME AS creatinine_date, 
						creatinine.value_as_number,
						datetime_diff(creatinine.measurement_DATETIME, index_date, DAY) as days_crea_index
					FROM {suspected_infection} AS susp_inf_rollup
					LEFT JOIN creatinine_from_measurement AS creatinine USING (person_id)
					WHERE
						CAST(index_date AS DATE) >= CAST(DATETIME_SUB(measurement_DATETIME, INTERVAL 2 DAY) AS DATE) AND
						CAST(index_date AS DATE) <= CAST(DATETIME_ADD(measurement_DATETIME, INTERVAL 1 DAY) AS DATE) AND
						value_as_number IS NOT NULL
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_rollup_query(self, format_query=True):
		query = '''
				creatinine_rollup AS (
					SELECT 
						person_id, 
						admit_date,
						admit_datetime,
						MAX(value_as_number) as max_creatinine
					FROM creatinine_window 
					GROUP BY person_id, admit_date, admit_datetime
				)
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

class GlasgowComaScaleComponent(Component): 
	'''
	Class to get glasgow coma scale score measurement from cohort.
	'''
	def __init__(self, prior=False, *args, **kwargs):
		Component.__init__(self, *args, **kwargs)
		self.prior = prior
	
	def get_component_query(self, format_query=True):
		query = '''
				CREATE OR REPLACE TABLE `{sepsis_gcs}` AS
				{values_query}
				{window_query}
				{rollup_query} 
				select * from gcs_rollup
				'''
		if not format_query:
			pass
		else:
			query = query.format_map({**self.config_dict,**{"values_query":self.get_values_query(), "window_query":self.get_window_query(), "rollup_query":self.get_rollup_query()}})
			
		if self.config_dict['print_query']:
			print(query)
			
		return query

	def get_values_query(self, format_query=True):
		query = '''
				WITH gcs_from_measurement AS (
					SELECT 
						measure.person_id, 
						measure.measurement_DATETIME, 
						measure.value_as_number, 
						concept.concept_name AS measure_type  
					FROM {dataset_project}.{dataset}.measurement AS measure
					INNER JOIN {dataset_project}.{dataset}.concept AS concept
					ON measure.measurement_concept_id = concept.concept_id
					WHERE concept.concept_id = 3032652 AND measure.value_as_number >= 3 AND measure.value_as_number <= 15
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_window_query(self, format_query=True):
		query = '''
				gcs_window AS (
					SELECT 
						susp_inf_rollup.*, 
						gcs.measurement_DATETIME AS gcs_date, 
						gcs.value_as_number,
						datetime_diff(gcs.measurement_DATETIME, index_date, DAY) as days_gcs_index
					FROM {suspected_infection} AS susp_inf_rollup
					LEFT JOIN gcs_from_measurement AS gcs USING (person_id)
					WHERE
						CAST(index_date AS DATE) >= CAST(DATETIME_SUB(measurement_DATETIME, INTERVAL 2 DAY) AS DATE) AND
						CAST(index_date AS DATE) <= CAST(DATETIME_ADD(measurement_DATETIME, INTERVAL 1 DAY) AS DATE)
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_rollup_query(self, format_query=True):
		query = '''
				gcs_rollup AS (
					SELECT 
						person_id, 
						admit_date,
						admit_datetime,
						MIN(value_as_number) as min_gcs
					FROM gcs_window 
					GROUP BY person_id, admit_date, admit_datetime
				)
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)
