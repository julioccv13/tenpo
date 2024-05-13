import json
import pandas as pd
import requests
import pyarrow
import datetime
import time
import logging
from core.google import bigquery as bq
from core.ports import Process
from core.spark import Spark

from google.cloud import bigquery


CREATE_QUERY_PATH = "gs://{bucket_name}/902_Zendesk/queries/01_transform_data.sql"


class ProcessZendesk(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._project = kwargs["project"] # tenpo-datalake-prod
        self._zendesk_api = kwargs["api_key"]
        self._zendesk_base_url = kwargs["base_url"]
        self._execution_date = pd.Timestamp.now()

    def run(self):
        try:
            self._logger_storage("Iniciando proceso Zendesk")
            self._logger_storage("PROJECTO SELF: " + self._project)
            self._api_call()
        except Exception as e:
            self._logger_storage("ERROR: " + str(e))
            raise Exception(e)

    def _logger_storage(self, text:str):
        """This function helps us to log to a cloud storage and monitor the main script """
        from google.cloud import storage as st
        client = st.Client(project=self._project)
        bucket = client.get_bucket(self._bucket_name)
        unix = str(time.time()).replace('.','_')
        bucket.blob(f'902_Zendesk/files/log_{unix}.txt').upload_from_string(text, 'text')

    
    # Función que nos entrega el dia de ayer en epoch.
    def _yesterday_epoch(self):
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        midnight_yesterday = datetime.datetime.combine(yesterday, datetime.time.min)
        epoch_yesterday = int(midnight_yesterday.timestamp())

        return epoch_yesterday
    
    
    def _get_incremental_users(self, url_ticket_base, token):
        # Inicializa el DataFrame final donde se almacenarán todos los usuarios
        df_final = pd.DataFrame()
        i = 0

        while url_ticket_base is not None:
            user = 'tech@tenpo.cl/token'
            response = requests.get(url_ticket_base, auth=(user, token))
            output = json.loads(response.content)
            users = output["users"]
            next_page = pd.json_normalize(output)
            users_normalized = pd.json_normalize(users)
            
            # Concatena el DataFrame actual de usuarios con el DataFrame final
            df_final = pd.concat([df_final, users_normalized], ignore_index=True)
            
            # Extrae la URL de la siguiente página si existe
            if 'after_url' in next_page:
                url_ticket_base = next_page["after_url"].iloc[0]
            else:
                url_ticket_base = None
            
            print(i)
            i += 1

        return df_final


    # Función que extrae los grupos de Zendesk.
    def _get_groups(self, url_ticket_base, token):
        # Inicializa el DataFrame final donde se almacenarán todos los grupos
        df_final = pd.DataFrame()
        i = 1
        next_page = True  # Se cambia a True para entrar al bucle la primera vez

        while next_page:
            url_ticket = url_ticket_base + 'groups.json?page=' + str(i)
            user = 'tech@tenpo.cl/token'
            response = requests.get(url_ticket, auth=(user, token))
            output = json.loads(response.content)
            
            # Normaliza los datos de grupos
            groups_normalized = pd.json_normalize(output["groups"])
            
            # Concatena el DataFrame actual de grupos con el DataFrame final
            df_final = pd.concat([df_final, groups_normalized], ignore_index=True)
            
            # Comprueba si hay una próxima página y actualiza el control del bucle
            next_page = output.get("next_page")
            i += 1
                
        return df_final
    
    def _get_incremental_tickets(self, url_ticket_base, token):
        # Inicializa el DataFrame final donde se almacenarán todos los tickets
        df_final = pd.DataFrame()

        while url_ticket_base is not None:
            user = 'tech@tenpo.cl/token'
            response = requests.get(url_ticket_base, auth=(user, token))
            output = json.loads(response.content)
            tickets_normalized = pd.json_normalize(output["tickets"])
            
            # Concatena el DataFrame actual de tickets con el DataFrame final
            df_final = pd.concat([df_final, tickets_normalized], ignore_index=True)

            # Verifica si hay una URL para la próxima página y actualiza url_ticket_base
            if 'after_url' in output:
                url_ticket_base = output["after_url"]
            else:
                url_ticket_base = None

        return df_final

    
    # Función que nos entrega toda la lista de ticket_fields.
    def _ticket_fields(self, url_ticket_base, token):
        # Inicializa el DataFrame final donde se almacenarán todos los ticket_fields
        df_final = pd.DataFrame()
        i = 1
        next_page = True  # Se cambia a True para entrar al bucle la primera vez

        while next_page:
            url_ticket = url_ticket_base + str(i)
            user = 'tech@tenpo.cl/token'
            response = requests.get(url_ticket, auth=(user, token))
            output = json.loads(response.content)
            ticket_fields_normalized = pd.json_normalize(output["ticket_fields"])
            
            # Concatena el DataFrame actual de ticket_fields con el DataFrame final
            df_final = pd.concat([df_final, ticket_fields_normalized], ignore_index=True)
            
            # Verifica si hay una próxima página y actualiza el control del bucle
            next_page = output.get("next_page")
            i += 1

        return df_final


    def _api_call(self):
        # PASO 1: declaramos las variables globales:
        project = self._project
        url_ticket_base = self._zendesk_base_url
        token = self._zendesk_api
        execution_date = self._execution_date


        self._logger_storage("PROJECTO GENERAL: " + project)

        # PASO 2: ejecutamos todo:
        data_types = {
            'url': 'object',
            'id': 'object',
            'external_id': 'object',
            'created_at': 'datetime64[ns]',
            'updated_at': 'datetime64[ns]',
            'type': 'object',
            'subject': 'object',
            'raw_subject': 'object',
            'description': 'object',
            'priority': 'object',
            'status': 'object',
            'recipient': 'object',
            'requester_id': 'object',
            'submitter_id': 'object',
            'assignee_id': 'object',
            'organization_id': 'object',
            'group_id': 'object',
            'collaborator_ids': 'object',
            'follower_ids': 'object',
            'email_cc_ids': 'object',
            'forum_topic_id': 'object',
            'problem_id': 'object',
            'has_incidents': 'object',
            'is_public': 'object',
            'due_at': 'object',
            'tags': 'object',
            'custom_fields': 'object',
            'sharing_agreement_ids': 'object',
            'custom_status_id': 'object',
            'fields': 'object',
            'followup_ids': 'object',
            'ticket_form_id': 'object',
            'brand_id': 'object',
            'allow_channelback': 'object',
            'allow_attachments': 'object',
            'from_messaging_channel': 'object',
            'generated_timestamp': 'object',
            'via_channel': 'object',
            'via_source_rel': 'object',
            'satisfaction_rating_score': 'object',
            'via_source_from_address': 'object',
            'via_source_from_name': 'object',
            'via_source_to_name': 'object',
            'via_source_to_address': 'object',
            'via_source_from_ticket_id': 'object',
            'via_source_from_subject': 'object',
            'via_source_from_channel': 'object',
            'satisfaction_rating_id': 'object',
            'satisfaction_rating_comment': 'object',
            'satisfaction_rating_reason': 'object',
            'satisfaction_rating_reason_id': 'object',
            'execution_date': 'object'
        }

        bq_schema = [
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("external_id", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("subject", "STRING"),
            bigquery.SchemaField("raw_subject", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("priority", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("recipient", "STRING"),
            bigquery.SchemaField("requester_id", "STRING"),
            bigquery.SchemaField("submitter_id", "STRING"),
            bigquery.SchemaField("assignee_id", "STRING"),
            bigquery.SchemaField("organization_id", "STRING"),
            bigquery.SchemaField("group_id", "STRING"),
            bigquery.SchemaField("collaborator_ids", "STRING"),
            bigquery.SchemaField("follower_ids", "STRING"),
            bigquery.SchemaField("email_cc_ids", "STRING"),
            bigquery.SchemaField("forum_topic_id", "STRING"),
            bigquery.SchemaField("problem_id", "STRING"),
            bigquery.SchemaField("has_incidents", "STRING"),
            bigquery.SchemaField("is_public", "STRING"),
            bigquery.SchemaField("due_at", "STRING"),
            bigquery.SchemaField("tags", "STRING"),
            bigquery.SchemaField("custom_fields", "STRING"),
            bigquery.SchemaField("sharing_agreement_ids", "STRING"),
            bigquery.SchemaField("custom_status_id", "STRING"),
            bigquery.SchemaField("fields", "STRING"),
            bigquery.SchemaField("followup_ids", "STRING"),
            bigquery.SchemaField("ticket_form_id", "STRING"),
            bigquery.SchemaField("brand_id", "STRING"),
            bigquery.SchemaField("allow_channelback", "STRING"),
            bigquery.SchemaField("allow_attachments", "STRING"),
            bigquery.SchemaField("from_messaging_channel", "STRING"),
            bigquery.SchemaField("generated_timestamp", "STRING"),
            bigquery.SchemaField("via_channel", "STRING"),
            bigquery.SchemaField("via_source_rel", "STRING"),
            bigquery.SchemaField("satisfaction_rating_score", "STRING"),
            bigquery.SchemaField("via_source_from_address", "STRING"),
            bigquery.SchemaField("via_source_from_name", "STRING"),
            bigquery.SchemaField("via_source_to_name", "STRING"),
            bigquery.SchemaField("via_source_to_address", "STRING"),
            bigquery.SchemaField("via_source_from_ticket_id", "STRING"),
            bigquery.SchemaField("via_source_from_subject", "STRING"),
            bigquery.SchemaField("via_source_from_channel", "STRING"),
            bigquery.SchemaField("satisfaction_rating_id", "STRING"),
            bigquery.SchemaField("satisfaction_rating_comment", "STRING"),
            bigquery.SchemaField("satisfaction_rating_reason", "STRING"),
            bigquery.SchemaField("satisfaction_rating_reason_id", "STRING"),
            bigquery.SchemaField("execution_date", "STRING")
        ]

        df_incremental_tickets_empty = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in data_types.items()})

        df_incremental_tickets = self._get_incremental_tickets(url_ticket_base + '/incremental/tickets/cursor.json?start_time=' + str(self._yesterday_epoch()), token)
        df_incremental_tickets["execution_date"] = execution_date
        df_incremental_tickets = df_incremental_tickets.astype(str)
        df_incremental_tickets.columns = df_incremental_tickets.columns.str.replace(".", "_")

        df_incremental_tickets = pd.concat([df_incremental_tickets_empty, df_incremental_tickets], ignore_index=True)
        df_incremental_tickets = df_incremental_tickets.astype(str)

        df_incremental_tickets["created_at"] = pd.to_datetime(df_incremental_tickets["created_at"])
        df_incremental_tickets["updated_at"] = pd.to_datetime(df_incremental_tickets["updated_at"])

        df_incremental_tickets = df_incremental_tickets[df_incremental_tickets_empty.columns]

        bq.reload_table("", project + ".zendesk.ingesta_incremental_tickets", bq_schema, df_incremental_tickets)



        data_types = {
        'url':'object',                  
        'id':'object',                   
        'type':'object',                 
        'title':'object',                
        'raw_title':'object',            
        'description':'object',          
        'raw_description':'object',      
        'position':'object',             
        'active':'object',               
        'required':'object',             
        'collapsed_for_agents':'object', 
        'regexp_for_validation':'object',
        'title_in_portal':'object',      
        'raw_title_in_portal':'object',  
        'visible_in_portal':'object',    
        'editable_in_portal':'object',   
        'required_in_portal':'object',   
        'tag':'object',                  
        'created_at':'datetime64[ns]',           
        'updated_at':'datetime64[ns]',           
        'removable':'object',            
        'key':'object',                  
        'agent_description':'object',
        'system_field_options':'object',
        'sub_type_id':'object',
        'custom_statuses':'object',
        'custom_field_options':'object',
        'execution_date':'object'
        }

        bq_schema = [
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("raw_title", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("raw_description", "STRING"),
            bigquery.SchemaField("position", "STRING"),
            bigquery.SchemaField("active", "STRING"),
            bigquery.SchemaField("required", "STRING"),
            bigquery.SchemaField("collapsed_for_agents", "STRING"),
            bigquery.SchemaField("regexp_for_validation", "STRING"),
            bigquery.SchemaField("title_in_portal", "STRING"),
            bigquery.SchemaField("raw_title_in_portal", "STRING"),
            bigquery.SchemaField("visible_in_portal", "STRING"),
            bigquery.SchemaField("editable_in_portal", "STRING"),
            bigquery.SchemaField("required_in_portal", "STRING"),
            bigquery.SchemaField("tag", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("removable", "STRING"),
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("agent_description", "STRING"),
            bigquery.SchemaField("system_field_options", "STRING"),
            bigquery.SchemaField("sub_type_id", "STRING"),
            bigquery.SchemaField("custom_statuses", "STRING"),
            bigquery.SchemaField("custom_field_options", "STRING"),
            bigquery.SchemaField("execution_date", "STRING")
        ]


        df_tickets_fields_empty = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in data_types.items()})

        df_tickets_fields = self._ticket_fields(url_ticket_base + 'ticket_fields.json?page=', token)
        df_tickets_fields["execution_date"] = execution_date
        df_tickets_fields = df_tickets_fields.astype(str)
        df_tickets_fields.columns = df_tickets_fields.columns.str.replace(".", "_")

        df_tickets_fields = pd.concat([df_tickets_fields_empty, df_tickets_fields], ignore_index=True)
        df_tickets_fields = df_tickets_fields.astype(str)

        df_tickets_fields["created_at"] = pd.to_datetime(df_tickets_fields["created_at"])
        df_tickets_fields["updated_at"] = pd.to_datetime(df_tickets_fields["updated_at"])


        df_tickets_fields = df_tickets_fields[df_tickets_fields_empty.columns]

        bq.reload_table("", project + ".zendesk.ingesta_tickets_fields", bq_schema, df_tickets_fields)


        data_types = {
        'id':'object',                                      
        'url':'object',                                     
        'name':'object',                                    
        'email':'object',                                   
        'created_at':'datetime64[ns]',                              
        'updated_at':'datetime64[ns]',                              
        'time_zone':'object',                               
        'iana_time_zone':'object',                          
        'phone':'object',                                   
        'shared_phone_number':'object',                     
        'photo':'object',                                   
        'locale_id':'object',                               
        'locale':'object',                                  
        'organization_id':'object',                         
        'role':'object',                                    
        'verified':'object',                                
        'external_id':'object',                             
        'tags':'object',                                    
        'alias':'object',                                   
        'active':'object',                                  
        'shared':'object',                                  
        'shared_agent':'object',                            
        'last_login_at':'object',                           
        'two_factor_auth_enabled':'object',                 
        'signature':'object',                               
        'details':'object',                                 
        'notes':'object',                                   
        'role_type':'object',                               
        'custom_role_id':'object',                          
        'moderator':'object',                               
        'ticket_restriction':'object',                      
        'only_private_comments':'object',                   
        'restricted_agent':'object',                        
        'suspended':'object',                               
        'chat_only':'object',                               
        'default_group_id':'object',                        
        'report_csv':'object',                              
        'user_fields_comuna':'object',                      
        'user_fields_direccion':'object',                   
        'user_fields_fecha_de_registro':'object',           
        'user_fields_nacionalidad':'object',                
        'user_fields_ocupacion':'object',                   
        'user_fields_region':'object',                      
        'user_fields_rut1':'object',                        
        'permanently_deleted':'object',                     
        'photo_url':'object',                               
        'photo_id':'object',                                
        'photo_file_name':'object',                         
        'photo_content_url':'object',                       
        'photo_mapped_content_url':'object',                
        'photo_content_type':'object',                      
        'photo_size':'object',                              
        'photo_width':'object',                             
        'photo_height':'object',                            
        'photo_inline':'object',                            
        'photo_deleted':'object',                           
        'photo_thumbnails':'object',                        
        'execution_date':'object'
        }

        bq_schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("time_zone", "STRING"),
            bigquery.SchemaField("iana_time_zone", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("shared_phone_number", "STRING"),
            bigquery.SchemaField("photo", "STRING"),
            bigquery.SchemaField("locale_id", "STRING"),
            bigquery.SchemaField("locale", "STRING"),
            bigquery.SchemaField("organization_id", "STRING"),
            bigquery.SchemaField("role", "STRING"),
            bigquery.SchemaField("verified", "STRING"),
            bigquery.SchemaField("external_id", "STRING"),
            bigquery.SchemaField("tags", "STRING"),
            bigquery.SchemaField("alias", "STRING"),
            bigquery.SchemaField("active", "STRING"),
            bigquery.SchemaField("shared", "STRING"),
            bigquery.SchemaField("shared_agent", "STRING"),
            bigquery.SchemaField("last_login_at", "STRING"),
            bigquery.SchemaField("two_factor_auth_enabled", "STRING"),
            bigquery.SchemaField("signature", "STRING"),
            bigquery.SchemaField("details", "STRING"),
            bigquery.SchemaField("notes", "STRING"),
            bigquery.SchemaField("role_type", "STRING"),
            bigquery.SchemaField("custom_role_id", "STRING"),
            bigquery.SchemaField("moderator", "STRING"),
            bigquery.SchemaField("ticket_restriction", "STRING"),
            bigquery.SchemaField("only_private_comments", "STRING"),
            bigquery.SchemaField("restricted_agent", "STRING"),
            bigquery.SchemaField("suspended", "STRING"),
            bigquery.SchemaField("chat_only", "STRING"),
            bigquery.SchemaField("default_group_id", "STRING"),
            bigquery.SchemaField("report_csv", "STRING"),
            bigquery.SchemaField("user_fields_comuna", "STRING"),
            bigquery.SchemaField("user_fields_direccion", "STRING"),
            bigquery.SchemaField("user_fields_fecha_de_registro", "STRING"),
            bigquery.SchemaField("user_fields_nacionalidad", "STRING"),
            bigquery.SchemaField("user_fields_ocupacion", "STRING"),
            bigquery.SchemaField("user_fields_region", "STRING"),
            bigquery.SchemaField("user_fields_rut1", "STRING"),
            bigquery.SchemaField("permanently_deleted", "STRING"),
            bigquery.SchemaField("photo_url", "STRING"),
            bigquery.SchemaField("photo_id", "STRING"),
            bigquery.SchemaField("photo_file_name", "STRING"),
            bigquery.SchemaField("photo_content_url", "STRING"),
            bigquery.SchemaField("photo_mapped_content_url", "STRING"),
            bigquery.SchemaField("photo_content_type", "STRING"),
            bigquery.SchemaField("photo_size", "STRING"),
            bigquery.SchemaField("photo_width", "STRING"),
            bigquery.SchemaField("photo_height", "STRING"),
            bigquery.SchemaField("photo_inline", "STRING"),
            bigquery.SchemaField("photo_deleted", "STRING"),
            bigquery.SchemaField("photo_thumbnails", "STRING"),
            bigquery.SchemaField("execution_date", "STRING")
        ]

        df_incremental_users_empty = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in data_types.items()})

        df_incremental_users = self._get_incremental_users(url_ticket_base + '/incremental/users/cursor.json?start_time=' + str(self._yesterday_epoch()), token)
        df_incremental_users["execution_date"] = execution_date
        df_incremental_users = df_incremental_users.astype(str)
        df_incremental_users.columns = df_incremental_users.columns.str.replace(".", "_")

        df_incremental_users = pd.concat([df_incremental_users_empty, df_incremental_users], ignore_index=True)
        df_incremental_users = df_incremental_users.astype(str)

        df_incremental_users["created_at"] = pd.to_datetime(df_incremental_users["created_at"])
        df_incremental_users["updated_at"] = pd.to_datetime(df_incremental_users["updated_at"])


        df_incremental_users = df_incremental_users[df_incremental_users_empty.columns]

        bq.reload_table("", project + ".zendesk.ingesta_incremental_users", bq_schema, df_incremental_users)


        data_types = {
        'url':'object',                
        'id':'object',                 
        'is_public':'object',          
        'name':'object',               
        'description':'object',        
        'default':'object',            
        'deleted':'object',            
        'created_at':'datetime64[ns]',         
        'updated_at':'datetime64[ns]',         
        'execution_date':'object'
        }

        bq_schema = [
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("is_public", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("default", "STRING"),
            bigquery.SchemaField("deleted", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("execution_date", "STRING")
        ]


        df_groups_empty = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in data_types.items()})

        df_groups = self._get_groups(url_ticket_base, token)
        df_groups["execution_date"] = execution_date
        df_groups = df_groups.astype(str)
        df_groups.columns = df_groups.columns.str.replace(".", "_")

        df_groups = pd.concat([df_groups_empty, df_groups], ignore_index=True)
        df_groups = df_groups.astype(str)

        df_groups["created_at"] = pd.to_datetime(df_groups["created_at"])
        df_groups["updated_at"] = pd.to_datetime(df_groups["updated_at"])

        df_groups = df_groups[df_groups_empty.columns]

        bq.reload_table("", project + ".zendesk.ingesta_incremental_groups", bq_schema, df_groups)