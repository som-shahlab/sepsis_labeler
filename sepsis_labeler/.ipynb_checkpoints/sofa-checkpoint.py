import os
from prediction_utils.extraction_utils.database import BQDatabase

class SOFAScore:
	def __init__(self, prior=False, *args, **kwargs):
		self.config_dict = self.get_config_dict(**kwargs)
		self.db = BQDatabase(**self.config_dict)
	
	def get_difference_query(self, format_query=True):
		query = '''
				CREATE OR REPLACE TABLE `{sepsis_difference}` AS
				{values_query}
				{window_query}
				{rollup_query} 
				select * from lactate_rollup
				'''
		if not format_query:
			pass
		else:
			query = query.format_map(
						{**{"values_query":self.get_values_query(), 
							"window_query":self.get_window_query(), 
							"rollup_query":self.get_rollup_query()}, 
							"sepsis_difference":self.config_dict['sepsis_difference']})
			
		if self.config_dict['print_query']:
			print(query)
			
		return query

	def get_score_query(self, format_query=True, prior=False):
		query = '''
				SELECT 
                susp_inf_rollup.person_id, 
                CAST(susp_inf_rollup.admit_date AS DATE) AS admit_date, 
                CAST(discharge_date AS DATE) AS discharge_date,
                min_bc, 
                min_systemic_abx, 
                index_date, 
                min_platelet, 
                CASE 
                    WHEN min_platelet IS NULL THEN 0 
                    WHEN min_platelet <20 THEN 4 
                    WHEN min_platelet < 50 THEN 3
                    WHEN min_platelet < 100 THEN 2 
                    WHEN min_platelet < 150 THEN 1 
                    ELSE 0 
                END plat_SOFA,
                max_bilirubin, 
                CASE 
                    WHEN max_bilirubin IS NULL THEN 0 
                    WHEN max_bilirubin >= 12 THEN 4 
                    WHEN max_bilirubin >= 6 THEN 3
                    WHEN max_bilirubin >= 2 THEN 2 
                    WHEN max_bilirubin >= 1.2 THEN 1 
                    ELSE 0
                END bili_SOFA,
                max_creatinine, 
                CASE 
                    WHEN max_creatinine IS NULL THEN 0 
                    WHEN max_creatinine >= 5 THEN 4 
                    WHEN max_creatinine >= 3.5 THEN 3
                    WHEN max_creatinine >= 2 THEN 2 
                    WHEN max_creatinine >= 1.2 THEN 1 
                    ELSE 0 
                END crea_SOFA,
                max_vaso_days, 
                min_map, 
                CASE 
                    WHEN max_vaso_days IS NOT NULL THEN 2
                    WHEN min_map <70 THEN 1 
                    ELSE 0 
                END cv_SOFA,
                min_paO2fiO2_ratio,
                min_spO2fiO2_ratio,
                count_vent_mode, 
                CASE 
                    WHEN (count_vent_mode IS NOT NULL AND min_paO2fiO2_ratio < 100) THEN 4
                    WHEN (count_vent_mode IS NOT NULL AND min_paO2fiO2_ratio < 200) THEN 3 
                    WHEN min_paO2fiO2_ratio < 300 THEN 2 
                    WHEN min_paO2fiO2_ratio < 400 THEN 1
                    ELSE 0 
                END pao2_resp_SOFA,
				CASE 
                    WHEN (count_vent_mode IS NOT NULL AND min_spO2fiO2_ratio < 148) THEN 4
                    WHEN (count_vent_mode IS NOT NULL AND min_spO2fiO2_ratio < 221) THEN 3 
                    WHEN min_spO2fiO2_ratio < 264 THEN 2 
                    WHEN min_spO2fiO2_ratio < 292 THEN 1
                    ELSE 0 
                END spo2_resp_SOFA,
                min_gcs, 
                CASE 
                    WHEN min_gcs IS NULL THEN 0 
                    WHEN min_gcs < 6 THEN 4 
                    WHEN min_gcs < 10 THEN 3
                    WHEN min_gcs < 13 THEN 2 
                    WHEN min_gcs < 15 THEN 1 
                    ELSE 0 
                END gcs_SOFA,
                min_urine_daily, 
                CASE 
                    WHEN min_urine_daily IS NULL THEN 0 
                    WHEN min_urine_daily < 200 THEN 4 
                    WHEN min_urine_daily < 500 THEN 3 
                    ELSE 0 
                END urine_SOFA,
                CASE 
                    WHEN max_vaso_days IS NOT NULL THEN 1 
                    ELSE 0 
                END vaso_shock,
                CASE 
                    WHEN min_map < 65 THEN 1 
                    ELSE 0 
                END map_shock,
                max_lactate, 
                CASE 
                    WHEN max_lactate > 2 THEN 1 
                    ELSE 0 
                END lact_shock
            FROM {suspected_infection} AS susp_inf_rollup
            LEFT JOIN {sepsis_platelet} USING (person_id, admit_date)
            LEFT JOIN {sepsis_bilirubin} USING (person_id, admit_date)
            LEFT JOIN {sepsis_creatinine} USING (person_id, admit_date)
            LEFT JOIN {sepsis_vasopressor} USING (person_id, admit_date)
            LEFT JOIN {sepsis_map} USING (person_id, admit_date)
            LEFT JOIN {sepsis_paO2_fiO2} USING (person_id, admit_date)
            LEFT JOIN {sepsis_vent} USING (person_id, admit_date)
            LEFT JOIN {sepsis_gcs} USING (person_id, admit_date)
            LEFT JOIN {sepsis_urine} USING (person_id, admit_date)
            LEFT JOIN {sepsis_lactate} USING (person_id, admit_date)
            LEFT JOIN {sepsis_spO2_fiO2} USING (person_id, admit_date)
            ORDER BY person_id, CAST(admit_date AS DATE)
		'''
		
		if not format_query:
			pass
		else:
			query = query.format_map(
						{**{"sepsis_platelet":self.config_dict['sepsis_platelet'] + '_prior' if self.prior else self.config_dict['sepsis_platelet'],
							"sepsis_creatinine":self.config_dict['sepsis_creatinine'] + '_prior' if self.prior else self.config_dict['sepsis_creatinine'],
							"sepsis_gcs":self.config_dict['sepsis_gcs'] + '_prior' if self.prior else self.config_dict['sepsis_gcs'],
							"sepsis_bilirubin":self.config_dict['sepsis_bilirubin'] + '_prior' if self.prior else self.config_dict['sepsis_bilirubin'],
							"sepsis_vent":self.config_dict['sepsis_vent'] + '_prior' if self.prior else self.config_dict['sepsis_vent'],
							"sepsis_lactate":self.config_dict['sepsis_lactate'] + '_prior' if self.prior else self.config_dict['sepsis_lactate'],
							"sepsis_spo2_fio2":self.config_dict['sepsis_spo2_fio2'] + '_prior' if self.prior else self.config_dict['sepsis_spo2_fio2'],
							"sepsis_vasopressor":self.config_dict['sepsis_vasopressor'] + '_prior' if self.prior else self.config_dict['sepsis_vasopressor'],
							"sepsis_map":self.config_dict['sepsis_map'] + '_prior' if self.prior else self.config_dict['sepsis_map'],
							"sepsis_urine":self.config_dict['sepsis_urine'] + '_prior' if self.prior else self.config_dict['sepsis_urine'],
							"sepsis_pao2_fio2":self.config_dict['sepsis_pao2_fio2'] + '_prior' if self.prior else self.config_dict['sepsis_pao2_fio2']}})
			
		if self.config_dict['print_query']:
			print(query)
			
		if self.config_dict['save_sofa']:
			self.db.execute_sql('''
								CREATE OR REPLACE TABLE `{sepsis_sofa}` AS
								{sofa_query}
								'''.format_map({**{
										"sepsis_sofa":self.config_dict["sepsis_sofa"] + '_prior' if self.prior else self.config_dict["sepsis_sofa"],
										"sofa_query": query 
									}}))
		return query
	def get_pediatric_query(self, prior=False):
		raise NotImplementedError
	
	def run_score_queries(self):
		self.db.execute_sql(self.get_score_query())
		self.db.execute_sql(self.get_score_query(prior=True))
	
	def run_pediatric_queries(self):
		self.db.execute_sql(self.get_pediatric_query())
		self.db.execute_sql(self.get_pediatric_query(prior=True))
	
	def create_sofa_table(self):
		self.run_score_queries()
		self.run_pediatric_queries()
		self.db.execute_sql(self.get_difference_query())

	def get_defaults(self):
		return {
			"gcloud_project": "som-nero-nigam-starr",
			"dataset_project": None,
			"rs_dataset_project": None,
			"dataset": "starr_omop_cdm5_deid_2022_08_01",
			"rs_dataset": "sepsis_temp_dataset",
			"cohort_name": "sepsis_temp_cohort",
			"ext_flwsht_table": "meas_vals_json",
			"google_application_credentials": os.path.expanduser(
				"~/.config/gcloud/application_default_credentials.json"
			),
			"limit": None,
			"min_stay_hour":0,
			"print_query":False,
			"save_sofa":True
		}

	def override_defaults(self, **kwargs):
		return {**self.get_defaults(), **kwargs}

	def get_config_dict(self, **kwargs):
		config_dict = self.override_defaults(**kwargs)

		# Handle special parameters
		config_dict["limit_str"] = (
			"LIMIT {}".format(config_dict["limit"])
			if (
				(config_dict["limit"] is not None)
				and (config_dict["limit"] != "")
				and (config_dict["limit"] != 0)
			)
			else ""
		)
		config_dict["where_str"] = (
			"WHERE DATETIME_DIFF(visit_end_datetime, visit_start_datetime, hour)>{}".format(
				str(config_dict["min_stay_hour"])
			)
			if (
				(config_dict["min_stay_hour"] is not None)
				and (config_dict["min_stay_hour"] != "")
			)
			else ""
		)
		config_dict["dataset_project"] = (
			config_dict["dataset_project"]
			if (
				(config_dict["dataset_project"] is not None)
				and (config_dict["dataset_project"] != "")
			)
			else config_dict["gcloud_project"]
		)
		config_dict["rs_dataset_project"] = (
			config_dict["rs_dataset_project"]
			if (
				(config_dict["rs_dataset_project"] is not None)
				and (config_dict["rs_dataset_project"] != "")
			)
			else config_dict["gcloud_project"]
		)
		return config_dict