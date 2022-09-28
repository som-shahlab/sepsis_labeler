import os
from prediction_utils.extraction_utils.database import BQDatabase

class Component:
	def __init__(self, *args, **kwargs):
		print('dsasadsad')
		self.config_dict = self.get_config_dict(**kwargs)
		# self.db = BQDatabase(**self.config_dict)

	def get_component_query(self):
		raise NotImplementedError

	def get_values_query(self):
		raise NotImplementedError

	def get_window_query(self):
		raise NotImplementedError

	def get_rollup_query(self):
		raise NotImplementedError


class PlateletComponent:
	'''
	Class to get platelet count component.
	'''
	def get_component(self, format_query=True):
		values_query = self.get_values()
		window_query = self.get_window()
		rollup_query = self.get_rollup()

		query = '''
				CREATE OR REPLACE TABLE {dataset_project}.{rs_dataset}.sepsis_platelet_rollup
				'''
				+ values_query + window_query + rollup_query +
				'''
				select * from platelet_rollup
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_values(self, format_query=True):
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
					)
				),
				'''
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

		def get_window(self, format_query=True):
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
					)
					'''
			if not format_query:
				return query
			else:
				return query.format_map(self.config_dict)

		def get_rollup(self, format_query=True):
			query = '''
					platelet_rollup AS (
						SELECT 
							person_id, 
							admit_date, 
							MIN(value_as_number) as min_platelet
						FROM platelet_window 
						GROUP BY person_id, admit_date
					)
					'''
			if not format_query:
				return query
			else:
				return query.format_map(self.config_dict)
