from prediction_utils.cohorts.cohort import BQCohort

class SepsisCohort(BQCohort):
	'''
	Class to get the sepsis admission cohort from OMOP schema.
	Note: Optional, can specify pre-existing admission rollup in labeler arguments.
	'''
	def get_base_query(self, format_query=True):
		query = """ (
		SELECT * FROM (
			SELECT 
				t1.person_id, 
				visit_concept_id, 
				visit_start_datetime, 
				visit_end_datetime
			FROM {dataset_project}.{dataset}.visit_occurrence t1
			INNER JOIN {dataset_project}.{dataset}.person as t2
				ON t1.person_id = t2.person_id
			WHERE
				visit_concept_id in (9201, 262)
				AND visit_end_datetime > visit_start_datetime
				AND visit_end_datetime is not NULL
				AND visit_start_datetime is not NULL
		)
		{where_str}
		{limit_str}
		)
		"""
		if not format_query:
			return query
		else:
			return query.format_map(self.config_dict)

	def get_transform_query(self, format_query=True):
		query = """
			WITH visits AS (
			  SELECT *
			  FROM {base_query}
			),
			visits_melt AS (
			  SELECT person_id, visit_start_datetime AS endpoint_date, 1 as endpoint_type
			  FROM visits
			  UNION ALL
			  SELECT person_id, visit_end_datetime AS endpoint_date, -1 as endpoint_type
			  FROM visits
			),
			counts1 AS (
			  SELECT *, COUNT(*) * endpoint_type as count
			  FROM visits_melt
			  GROUP BY person_id, endpoint_date, endpoint_type
			),
			counts2 AS (
			  SELECT person_id, endpoint_date, SUM(count) as count
			  FROM counts1
			  GROUP BY person_id, endpoint_date
			),
			counts3 AS (
			  SELECT person_id, endpoint_date,
				  SUM(count) OVER(PARTITION BY person_id ORDER BY endpoint_date) as count
			  FROM counts2
			),
			cum_counts AS (
			  SELECT *, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY endpoint_date) as row_number
			  FROM counts3
			),
			discharge_times AS (
			  SELECT person_id, endpoint_date, 'discharge_date' as endpoint_type, row_number
			  FROM cum_counts
			  WHERE count = 0
			),
			discharge_times_row_shifted AS (
			  SELECT person_id, (row_number + 1) as row_number
			  FROM discharge_times
			),
			first_admit_times AS (
			  SELECT person_id, endpoint_date, 'admit_date' as endpoint_type
			  FROM cum_counts
			  WHERE row_number = 1
			),
			other_admit_times AS (
			  SELECT t1.person_id, endpoint_date, 'admit_date' as endpoint_type
			  FROM cum_counts t1
			  INNER JOIN discharge_times_row_shifted AS t2
			  ON t1.person_id=t2.person_id AND t1.row_number=t2.row_number
			),
			aggregated_endpoints AS (
			  SELECT person_id, endpoint_date, endpoint_type
			  FROM discharge_times
			  UNION ALL
			  SELECT person_id, endpoint_date, endpoint_type
			  FROM first_admit_times
			  UNION ALL
			  SELECT person_id, endpoint_date, endpoint_type
			  FROM other_admit_times
			),
			result_long AS (
			  SELECT *, ROW_NUMBER() OVER(PARTITION BY person_id, endpoint_type ORDER BY endpoint_date) as row_number
			  FROM aggregated_endpoints
			),
			discharge_times_final AS (
				SELECT person_id, endpoint_date as discharge_date, row_number
				FROM result_long
				WHERE endpoint_type = 'discharge_date'
			),
			admit_times_final AS (
				SELECT person_id, endpoint_date as admit_date, row_number
				FROM result_long
				WHERE endpoint_type = 'admit_date'
			),
			result AS (
				SELECT t1.person_id, admit_date, discharge_date, t1.row_number
				FROM admit_times_final t1
				INNER JOIN discharge_times_final as t2
				ON t1.person_id=t2.person_id AND t1.row_number=t2.row_number
			)
			SELECT person_id, admit_date, discharge_date
			FROM result
			ORDER BY person_id, row_number
		"""

		if not format_query:
			return query
		else:
			return query.format_map(
				{**self.config_dict, **{"base_query": self.get_base_query()}}
			)

	def get_create_query(self, format_query=True):

		query = """ 
			CREATE OR REPLACE TABLE {admission_rollup} AS
			{query}
		"""

		if not format_query:
			return query
		else:
			return query.format_map(
				{**self.config_dict, **{"query": self.get_transform_query()}}
			)