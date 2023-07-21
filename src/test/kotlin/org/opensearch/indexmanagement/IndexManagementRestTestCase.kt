/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.AfterClass
import org.junit.Before
import org.junit.rules.DisableOnDebug
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.client.RequestOptions
import org.opensearch.client.WarningsHandler
import org.opensearch.client.ResponseException
import org.opensearch.common.Strings
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.rest.RestStatus
import java.io.IOException
import java.nio.file.Files
import java.util.*
import javax.management.MBeanServerInvocationHandler
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL
import kotlin.collections.ArrayList
import kotlin.collections.HashSet

abstract class IndexManagementRestTestCase : ODFERestTestCase() {

    val configSchemaVersion = 18
    val historySchemaVersion = 5

    // Having issues with tests leaking into other tests and mappings being incorrect and they are not caught by any pending task wait check as
    // they do not go through the pending task queue. Ideally this should probably be written in a way to wait for the
    // jobs themselves to finish and gracefully shut them down.. but for now seeing if this works.
    @Before
    fun setAutoCreateIndex() {
        client().makeRequest(
            "PUT", "_cluster/settings",
            StringEntity("""{"persistent":{"action.auto_create_index":"-.opendistro-*,*"}}""", ContentType.APPLICATION_JSON)
        )
    }

    // Tests on lower resource machines are experiencing flaky failures due to attempting to force a job to
    // start before the job scheduler has registered the index operations listener. Initializing the index
    // preemptively seems to give the job scheduler time to listen to operations.
    @Before
    fun initializeManagedIndex() {
        if (!isIndexExists(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)) {
            val request = Request("PUT", "/${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}")
            var entity = "{\"settings\": " + Strings.toString(XContentType.JSON, Settings.builder().put(INDEX_HIDDEN, true).build())
            entity += ",\"mappings\" : ${IndexManagementIndices.indexManagementMappings}}"
            request.setJsonEntity(entity)
            client().performRequest(request)
        }
    }

    protected val isDebuggingTest = DisableOnDebug(null).isDebugging
    protected val isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false")!!.toBoolean()

    protected val isLocalTest = clusterName() == "integTest"
    private fun clusterName(): String {
        return System.getProperty("tests.clustername")
    }

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun isIndexExists(index: String): Boolean {
        val response = client().makeRequest("HEAD", index)
        return RestStatus.OK == response.restStatus()
    }

    protected fun assertIndexExists(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }

    protected fun assertIndexDoesNotExist(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does exist.", RestStatus.NOT_FOUND, response.restStatus())
    }

    protected fun verifyIndexSchemaVersion(index: String, expectedVersion: Int) {
        val indexMapping = client().getIndexMapping(index)
        val indexName = indexMapping.keys.toList()[0]
        val mappings = indexMapping.stringMap(indexName)?.stringMap("mappings")
        var version = 0
        if (mappings!!.containsKey("_meta")) {
            val meta = mappings.stringMap("_meta")
            if (meta!!.containsKey("schema_version")) version = meta.get("schema_version") as Int
        }
        assertEquals(expectedVersion, version)
    }

    @Suppress("UNCHECKED_CAST")
    fun Map<String, Any>.stringMap(key: String): Map<String, Any>? {
        val map = this as Map<String, Map<String, Any>>
        return map[key]
    }

    fun RestClient.getIndexMapping(index: String): Map<String, Any> {
        val response = this.makeRequest("GET", "$index/_mapping")
        assertEquals(RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    /**
     * Inserts [docCount] sample documents into [index], optionally waiting [delay] milliseconds
     * in between each insertion
     */
    protected fun insertSampleData(index: String, docCount: Int, delay: Long = 0, jsonString: String = "{ \"test_field\": \"test_value\" }", routing: String? = null) {
        var endpoint = "/$index/_doc/?refresh=true"
        if (routing != null) endpoint += "&routing=$routing"
        repeat(docCount) {
            val request = Request("POST", endpoint)
            request.setJsonEntity(jsonString)
            client().performRequest(request)

            Thread.sleep(delay)
        }
    }

    protected fun insertSampleBulkData(index: String, bulkJsonString: String) {
        val request = Request("POST", "/$index/_bulk/?refresh=true")
        request.setJsonEntity(bulkJsonString)
        request.options = RequestOptions.DEFAULT.toBuilder().addHeader("content-type", "application/x-ndjson").build()
        val res = client().performRequest(request)
        assertEquals(RestStatus.OK, res.restStatus())
    }

    /**
     * Indexes 5k documents of the open NYC taxi dataset
     *
     * Example headers and document values
     * VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
     * 1,2019-01-01 00:46:40,2019-01-01 00:53:20,1,1.50,1,N,151,239,1,7,0.5,0.5,1.65,0,0.3,9.95,
     */
    protected fun generateNYCTaxiData(index: String = "nyc-taxi-data") {
        createIndex(index, Settings.EMPTY, """"properties":{"DOLocationID":{"type":"integer"},"RatecodeID":{"type":"integer"},"fare_amount":{"type":"float"},"tpep_dropoff_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"congestion_surcharge":{"type":"float"},"VendorID":{"type":"integer"},"passenger_count":{"type":"integer"},"tolls_amount":{"type":"float"},"improvement_surcharge":{"type":"float"},"trip_distance":{"type":"float"},"store_and_fwd_flag":{"type":"keyword"},"payment_type":{"type":"integer"},"total_amount":{"type":"float"},"extra":{"type":"float"},"tip_amount":{"type":"float"},"mta_tax":{"type":"float"},"tpep_pickup_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"PULocationID":{"type":"integer"}}""")
        insertSampleBulkData(index, javaClass.classLoader.getResource("data/nyc_5000.ndjson").readText())
    }

    protected fun generateMessageLogsData(index: String = "message-logs") {
        createIndex(index, Settings.EMPTY, """"properties": {"USM_Storage":{"properties":{"occured_to_received":{"type":"long"},"received_to_storage":{"type":"long"},"storage_to_os":{"type":"long"}}},"message":{"properties":{"access_control_outcome":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"access_key_id":{"ignore_above":10000,"type":"keyword"},"account_id":{"ignore_above":10000,"type":"keyword"},"account_name":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"account_vendor":{"ignore_above":10000,"type":"keyword"},"adhoc_query_id":{"ignore_above":10000,"type":"keyword"},"affected_family":{"ignore_above":10000,"type":"keyword"},"affected_platform":{"ignore_above":10000,"type":"keyword"},"affected_platforms":{"ignore_above":10000,"type":"keyword"},"affected_products":{"ignore_above":10000,"type":"keyword"},"alarm_events_count":{"type":"long"},"app_id":{"ignore_above":10000,"type":"keyword"},"app_name":{"ignore_above":10000,"type":"keyword"},"app_type":{"ignore_above":10000,"type":"keyword"},"application":{"ignore_above":10000,"type":"keyword"},"application_protocol":{"ignore_above":10000,"type":"keyword"},"application_type":{"ignore_above":10000,"type":"keyword"},"asset_status":{"ignore_above":10000,"type":"keyword"},"assumed_role":{"ignore_above":10000,"type":"keyword"},"audit_reason":{"ignore_above":10000,"type":"keyword"},"authentication_mode":{"ignore_above":10000,"type":"keyword"},"authentication_package_name":{"ignore_above":10000,"type":"keyword"},"authentication_type":{"ignore_above":10000,"type":"keyword"},"base_event_count":{"type":"long"},"blacklist_reference_url":{"ignore_above":10000,"type":"keyword"},"bytes_in":{"type":"long"},"bytes_out":{"type":"long"},"certificate_issuer_name":{"ignore_above":10000,"type":"keyword"},"certificate_serial_number":{"ignore_above":10000,"type":"keyword"},"certificate_subject_name":{"ignore_above":10000,"type":"keyword"},"confidence":{"type":"long"},"connection_count":{"type":"long"},"connector_id":{"ignore_above":10000,"type":"keyword"},"connector_source":{"ignore_above":10000,"type":"keyword"},"connector_source_file":{"ignore_above":10000,"type":"keyword"},"container_cmd":{"ignore_above":10000,"type":"keyword"},"container_cpu":{"ignore_above":10000,"type":"keyword"},"container_id":{"ignore_above":10000,"type":"keyword"},"container_image":{"ignore_above":10000,"type":"keyword"},"container_image_id":{"ignore_above":10000,"type":"keyword"},"container_memory":{"ignore_above":10000,"type":"keyword"},"container_name":{"ignore_above":10000,"type":"keyword"},"container_state":{"ignore_above":10000,"type":"keyword"},"container_volume":{"ignore_above":10000,"type":"keyword"},"contains_credit_card_number":{"type":"boolean"},"content_category":{"ignore_above":10000,"type":"keyword"},"control_id":{"ignore_above":10000,"type":"keyword"},"current_pps":{"type":"long"},"current_working_directory":{"ignore_above":10000,"type":"keyword"},"customfield_0":{"ignore_above":10000,"type":"keyword"},"customfield_1":{"ignore_above":10000,"type":"keyword"},"customfield_10":{"ignore_above":10000,"type":"keyword"},"customfield_11":{"ignore_above":10000,"type":"keyword"},"customfield_12":{"ignore_above":10000,"type":"keyword"},"customfield_13":{"ignore_above":10000,"type":"keyword"},"customfield_14":{"ignore_above":10000,"type":"keyword"},"customfield_15":{"ignore_above":10000,"type":"keyword"},"customfield_16":{"ignore_above":10000,"type":"keyword"},"customfield_17":{"ignore_above":10000,"type":"keyword"},"customfield_18":{"ignore_above":10000,"type":"keyword"},"customfield_19":{"ignore_above":10000,"type":"keyword"},"customfield_2":{"ignore_above":10000,"type":"keyword"},"customfield_20":{"ignore_above":10000,"type":"keyword"},"customfield_21":{"ignore_above":10000,"type":"keyword"},"customfield_22":{"ignore_above":10000,"type":"keyword"},"customfield_23":{"ignore_above":10000,"type":"keyword"},"customfield_24":{"ignore_above":10000,"type":"keyword"},"customfield_25":{"ignore_above":10000,"type":"keyword"},"customfield_26":{"ignore_above":10000,"type":"keyword"},"customfield_27":{"ignore_above":10000,"type":"keyword"},"customfield_28":{"ignore_above":10000,"type":"keyword"},"customfield_29":{"ignore_above":10000,"type":"keyword"},"customfield_3":{"ignore_above":10000,"type":"keyword"},"customfield_30":{"ignore_above":10000,"type":"keyword"},"customfield_4":{"ignore_above":10000,"type":"keyword"},"customfield_5":{"ignore_above":10000,"type":"keyword"},"customfield_6":{"ignore_above":10000,"type":"keyword"},"customfield_7":{"ignore_above":10000,"type":"keyword"},"customfield_8":{"ignore_above":10000,"type":"keyword"},"customfield_9":{"ignore_above":10000,"type":"keyword"},"customheader_0":{"ignore_above":10000,"type":"keyword"},"customheader_1":{"ignore_above":10000,"type":"keyword"},"customheader_10":{"ignore_above":10000,"type":"keyword"},"customheader_11":{"ignore_above":10000,"type":"keyword"},"customheader_12":{"ignore_above":10000,"type":"keyword"},"customheader_13":{"ignore_above":10000,"type":"keyword"},"customheader_14":{"ignore_above":10000,"type":"keyword"},"customheader_15":{"ignore_above":10000,"type":"keyword"},"customheader_16":{"ignore_above":10000,"type":"keyword"},"customheader_17":{"ignore_above":10000,"type":"keyword"},"customheader_18":{"ignore_above":10000,"type":"keyword"},"customheader_19":{"ignore_above":10000,"type":"keyword"},"customheader_2":{"ignore_above":10000,"type":"keyword"},"customheader_20":{"ignore_above":10000,"type":"keyword"},"customheader_21":{"ignore_above":10000,"type":"keyword"},"customheader_22":{"ignore_above":10000,"type":"keyword"},"customheader_23":{"ignore_above":10000,"type":"keyword"},"customheader_24":{"ignore_above":10000,"type":"keyword"},"customheader_25":{"ignore_above":10000,"type":"keyword"},"customheader_26":{"ignore_above":10000,"type":"keyword"},"customheader_27":{"ignore_above":10000,"type":"keyword"},"customheader_28":{"ignore_above":10000,"type":"keyword"},"customheader_29":{"ignore_above":10000,"type":"keyword"},"customheader_3":{"ignore_above":10000,"type":"keyword"},"customheader_30":{"ignore_above":10000,"type":"keyword"},"customheader_4":{"ignore_above":10000,"type":"keyword"},"customheader_5":{"ignore_above":10000,"type":"keyword"},"customheader_6":{"ignore_above":10000,"type":"keyword"},"customheader_7":{"ignore_above":10000,"type":"keyword"},"customheader_8":{"ignore_above":10000,"type":"keyword"},"customheader_9":{"ignore_above":10000,"type":"keyword"},"datascience_alarm_threshold":{"type":"float"},"datascience_alarm_threshold_99":{"type":"float"},"datascience_alarm_threshold_low_confidence":{"type":"float"},"datascience_alarm_threshold_medium_confidence":{"type":"float"},"datascience_anomaly_score":{"type":"float"},"datascience_inference_explanation":{"ignore_above":10000,"type":"keyword"},"datascience_inference_type":{"ignore_above":10000,"type":"keyword"},"datascience_tenant_event_threshold":{"type":"float"},"destination_account_id":{"ignore_above":10000,"type":"keyword"},"destination_additional_hostnames":{"ignore_above":10000,"type":"keyword"},"destination_address":{"ignore_above":10000,"type":"keyword"},"destination_address_6":{"ignore_above":10000,"type":"keyword"},"destination_asn":{"ignore_above":10000,"type":"keyword"},"destination_asset_id":{"ignore_above":10000,"type":"keyword"},"destination_blacklist_activity":{"ignore_above":10000,"type":"keyword"},"destination_blacklist_priority":{"ignore_above":10000,"type":"keyword"},"destination_blacklist_reliability":{"ignore_above":10000,"type":"keyword"},"destination_canonical":{"ignore_above":10000,"type":"keyword"},"destination_city":{"ignore_above":10000,"type":"keyword"},"destination_country":{"ignore_above":10000,"type":"keyword"},"destination_datastore":{"ignore_above":10000,"type":"keyword"},"destination_dns_domain":{"ignore_above":10000,"type":"keyword"},"destination_fqdn":{"ignore_above":10000,"type":"keyword"},"destination_hostname":{"ignore_above":10000,"type":"keyword"},"destination_infrastructure_name":{"ignore_above":10000,"type":"keyword"},"destination_infrastructure_type":{"ignore_above":10000,"type":"keyword"},"destination_instance_id":{"ignore_above":10000,"type":"keyword"},"destination_latitude":{"ignore_above":10000,"type":"keyword"},"destination_longitude":{"ignore_above":10000,"type":"keyword"},"destination_mac":{"ignore_above":10000,"type":"keyword"},"destination_mac_vendor":{"ignore_above":10000,"type":"keyword"},"destination_name":{"ignore_above":10000,"type":"keyword"},"destination_nat_address":{"ignore_above":10000,"type":"keyword"},"destination_nat_port":{"type":"long"},"destination_netmask":{"ignore_above":10000,"type":"keyword"},"destination_network":{"ignore_above":10000,"type":"keyword"},"destination_ntdomain":{"ignore_above":10000,"type":"keyword"},"destination_organisation":{"ignore_above":10000,"type":"keyword"},"destination_port":{"type":"long"},"destination_port_label":{"ignore_above":10000,"type":"keyword"},"destination_post_nat_port":{"type":"long"},"destination_pre_nat_port":{"type":"long"},"destination_process":{"ignore_above":10000,"type":"keyword"},"destination_process_id":{"ignore_above":10000,"type":"keyword"},"destination_region":{"ignore_above":10000,"type":"keyword"},"destination_registered_country":{"ignore_above":10000,"type":"keyword"},"destination_service_name":{"ignore_above":10000,"type":"keyword"},"destination_translated_address":{"ignore_above":10000,"type":"keyword"},"destination_translated_port":{"type":"long"},"destination_user_email":{"ignore_above":10000,"type":"keyword"},"destination_user_group":{"ignore_above":10000,"type":"keyword"},"destination_user_id":{"ignore_above":10000,"type":"keyword"},"destination_user_privileges":{"ignore_above":10000,"type":"keyword"},"destination_userid":{"ignore_above":10000,"type":"keyword"},"destination_username":{"ignore_above":10000,"type":"keyword"},"destination_vguest":{"ignore_above":10000,"type":"keyword"},"destination_vhost":{"ignore_above":10000,"type":"keyword"},"destination_vpc":{"ignore_above":10000,"type":"keyword"},"destination_vpn":{"ignore_above":10000,"type":"keyword"},"destination_zone":{"ignore_above":10000,"type":"keyword"},"device_class":{"ignore_above":10000,"type":"keyword"},"device_configuration":{"ignore_above":10000,"type":"keyword"},"device_custom_date_1":{"ignore_above":10000,"type":"keyword"},"device_custom_date_1_label":{"ignore_above":10000,"type":"keyword"},"device_custom_date_2":{"ignore_above":10000,"type":"keyword"},"device_custom_date_2_label":{"ignore_above":10000,"type":"keyword"},"device_custom_number_1":{"type":"long"},"device_custom_number_1_label":{"ignore_above":10000,"type":"keyword"},"device_custom_number_2":{"type":"long"},"device_custom_number_2_label":{"ignore_above":10000,"type":"keyword"},"device_custom_number_3":{"type":"long"},"device_custom_number_3_label":{"ignore_above":10000,"type":"keyword"},"device_direction":{"ignore_above":10000,"type":"keyword"},"device_dns_domain":{"ignore_above":10000,"type":"keyword"},"device_event_category":{"ignore_above":10000,"type":"keyword"},"device_external_id":{"ignore_above":10000,"type":"keyword"},"device_facility":{"ignore_above":10000,"type":"keyword"},"device_inbound_interface":{"ignore_above":10000,"type":"keyword"},"device_name":{"ignore_above":10000,"type":"keyword"},"device_nt_domain":{"ignore_above":10000,"type":"keyword"},"device_outbound_interface":{"ignore_above":10000,"type":"keyword"},"device_process_name":{"ignore_above":10000,"type":"keyword"},"device_sender_address":{"ignore_above":10000,"type":"keyword"},"device_sender_asset_id":{"ignore_above":10000,"type":"keyword"},"device_vendor":{"ignore_above":10000,"type":"keyword"},"dns_message":{"ignore_above":10000,"type":"keyword"},"dns_rcode":{"type":"long"},"dns_rrname":{"ignore_above":10000,"type":"keyword"},"dns_rrtype":{"ignore_above":10000,"type":"keyword"},"dns_server_address":{"ignore_above":10000,"type":"keyword"},"dns_ttl":{"ignore_above":10000,"type":"keyword"},"dns_type":{"ignore_above":10000,"type":"keyword"},"duration":{"ignore_above":10000,"type":"keyword"},"email_recipient":{"ignore_above":10000,"type":"keyword"},"email_relay":{"ignore_above":10000,"type":"keyword"},"email_sender":{"ignore_above":10000,"type":"keyword"},"email_subject":{"ignore_above":10000,"type":"keyword"},"environment_variable_key":{"ignore_above":10000,"type":"keyword"},"environment_variable_value":{"ignore_above":10000,"type":"keyword"},"error_code":{"ignore_above":10000,"type":"keyword"},"error_message":{"ignore_above":10000,"type":"keyword"},"event_action":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"event_activity":{"ignore_above":10000,"type":"keyword"},"event_attack_id":{"ignore_above":10000,"type":"keyword"},"event_attack_tactic":{"ignore_above":10000,"type":"keyword"},"event_attack_technique":{"ignore_above":10000,"type":"keyword"},"event_auth_action":{"ignore_above":10000,"type":"keyword"},"event_auth_role":{"ignore_above":10000,"type":"keyword"},"event_category":{"ignore_above":10000,"type":"keyword"},"event_cve":{"ignore_above":10000,"type":"keyword"},"event_description":{"ignore_above":10000,"type":"keyword"},"event_description_url":{"ignore_above":10000,"type":"keyword"},"event_group":{"ignore_above":10000,"type":"keyword"},"event_name":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"event_outcome":{"ignore_above":10000,"type":"keyword"},"event_priority":{"type":"long"},"event_receipt_time":{"type":"date"},"event_ref_date":{"ignore_above":10000,"type":"keyword"},"event_ref_score":{"ignore_above":10000,"type":"keyword"},"event_ref_source":{"ignore_above":10000,"type":"keyword"},"event_severity":{"ignore_above":10000,"type":"keyword"},"event_subcategory":{"ignore_above":10000,"type":"keyword"},"event_type":{"ignore_above":10000,"type":"keyword"},"event_violation":{"ignore_above":10000,"type":"keyword"},"expires":{"type":"boolean"},"external_id":{"ignore_above":10000,"type":"keyword"},"file_hash":{"ignore_above":10000,"type":"keyword"},"file_hash_algorithm":{"ignore_above":10000,"type":"keyword"},"file_hash_md5":{"ignore_above":10000,"type":"keyword"},"file_hash_sha1":{"ignore_above":10000,"type":"keyword"},"file_hash_sha256":{"ignore_above":10000,"type":"keyword"},"file_id":{"ignore_above":10000,"type":"keyword"},"file_kb_size":{"ignore_above":10000,"type":"keyword"},"file_modification_time":{"ignore_above":10000,"type":"keyword"},"file_name":{"ignore_above":10000,"type":"keyword"},"file_old_hash":{"ignore_above":10000,"type":"keyword"},"file_old_id":{"ignore_above":10000,"type":"keyword"},"file_old_modification_time":{"ignore_above":10000,"type":"keyword"},"file_old_name":{"ignore_above":10000,"type":"keyword"},"file_old_path":{"ignore_above":10000,"type":"keyword"},"file_old_permission":{"ignore_above":10000,"type":"keyword"},"file_old_size":{"ignore_above":10000,"type":"keyword"},"file_owner":{"ignore_above":10000,"type":"keyword"},"file_path":{"ignore_above":10000,"type":"keyword"},"file_permission":{"ignore_above":10000,"type":"keyword"},"file_type":{"ignore_above":10000,"type":"keyword"},"full_message":{"ignore_above":10000,"type":"keyword"},"gateway":{"ignore_above":10000,"type":"keyword"},"global_list_name":{"ignore_above":10000,"type":"keyword"},"global_list_value":{"ignore_above":10000,"type":"keyword"},"group_policy":{"ignore_above":10000,"type":"keyword"},"has_alarm":{"type":"boolean"},"highlight_fields":{"ignore_above":10000,"type":"keyword"},"http_hostname":{"ignore_above":10000,"type":"keyword"},"http_referer":{"ignore_above":10000,"type":"keyword"},"identity_group_name":{"ignore_above":10000,"type":"keyword"},"identity_host_name":{"ignore_above":10000,"type":"keyword"},"incident_id":{"ignore_above":10000,"type":"keyword"},"instance_ids":{"ignore_above":10000,"type":"keyword"},"instance_types":{"ignore_above":10000,"type":"keyword"},"iocs":{"ignore_above":10000,"type":"keyword"},"ip_addresses":{"ignore_above":10000,"type":"keyword"},"k8s_dns_policy":{"ignore_above":10000,"type":"keyword"},"k8s_node_name":{"ignore_above":10000,"type":"keyword"},"k8s_priority":{"ignore_above":10000,"type":"keyword"},"level":{"type":"long"},"log":{"index_options":"docs","norms":false,"type":"text"},"malware_family":{"ignore_above":10000,"type":"keyword"},"malware_variant":{"ignore_above":10000,"type":"keyword"},"matched_value":{"ignore_above":10000,"type":"keyword"},"needs_enrichment":{"type":"boolean"},"needs_internal_enrichment":{"type":"boolean"},"num_containers":{"ignore_above":10000,"type":"keyword"},"old_ip":{"ignore_above":10000,"type":"keyword"},"operating_system":{"ignore_above":10000,"type":"keyword"},"package_architecture":{"ignore_above":10000,"type":"keyword"},"package_name":{"ignore_above":10000,"type":"keyword"},"package_revision":{"ignore_above":10000,"type":"keyword"},"package_source":{"ignore_above":10000,"type":"keyword"},"package_version":{"ignore_above":10000,"type":"keyword"},"packet_data":{"ignore_above":10000,"type":"keyword"},"packet_payload":{"ignore_above":10000,"type":"keyword"},"packet_type":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"packets_received":{"type":"long"},"packets_sent":{"type":"long"},"peak_pps":{"type":"long"},"pefile_company":{"ignore_above":10000,"type":"keyword"},"pefile_description":{"ignore_above":10000,"type":"keyword"},"pefile_fileversion":{"ignore_above":10000,"type":"keyword"},"pefile_product":{"ignore_above":10000,"type":"keyword"},"plugin":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"plugin_device":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"plugin_device_type":{"ignore_above":10000,"type":"keyword"},"plugin_device_version":{"ignore_above":10000,"type":"keyword"},"plugin_enrichment_script":{"ignore_above":10000,"type":"keyword"},"plugin_family":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"plugin_parent":{"ignore_above":10000,"type":"keyword"},"plugin_rule":{"ignore_above":10000,"type":"keyword"},"plugin_version":{"ignore_above":10000,"type":"keyword"},"policy":{"ignore_above":10000,"type":"keyword"},"policy_address":{"ignore_above":10000,"type":"keyword"},"pre_authentication_type":{"ignore_above":10000,"type":"keyword"},"project_id":{"ignore_above":10000,"type":"keyword"},"protocol_version":{"ignore_above":10000,"type":"keyword"},"received_from":{"ignore_above":10000,"type":"keyword"},"registry_path":{"ignore_above":10000,"type":"keyword"},"registry_value":{"ignore_above":10000,"type":"keyword"},"relative_distinguished_name":{"ignore_above":10000,"type":"keyword"},"rep_dev_canonical":{"ignore_above":10000,"type":"keyword"},"rep_device_address":{"ignore_above":10000,"type":"keyword"},"rep_device_address_6":{"ignore_above":10000,"type":"keyword"},"rep_device_asset_id":{"ignore_above":10000,"type":"keyword"},"rep_device_fqdn":{"ignore_above":10000,"type":"keyword"},"rep_device_hostname":{"ignore_above":10000,"type":"keyword"},"rep_device_inbound_interface":{"ignore_above":10000,"type":"keyword"},"rep_device_instance_id":{"ignore_above":10000,"type":"keyword"},"rep_device_mac":{"ignore_above":10000,"type":"keyword"},"rep_device_model":{"ignore_above":10000,"type":"keyword"},"rep_device_outbound_interface":{"ignore_above":10000,"type":"keyword"},"rep_device_rule_id":{"ignore_above":10000,"type":"keyword"},"rep_device_type":{"ignore_above":10000,"type":"keyword"},"rep_device_vendor":{"ignore_above":10000,"type":"keyword"},"rep_device_version":{"ignore_above":10000,"type":"keyword"},"report_executed_date":{"type":"date"},"reputation_score":{"ignore_above":10000,"type":"keyword"},"request_content_type":{"ignore_above":10000,"type":"keyword"},"request_cookies":{"ignore_above":10000,"type":"keyword"},"request_http_version":{"ignore_above":10000,"type":"keyword"},"request_method":{"ignore_above":10000,"type":"keyword"},"request_referrer":{"ignore_above":10000,"type":"keyword"},"request_url":{"ignore_above":10000,"type":"keyword"},"request_user_agent":{"ignore_above":10000,"type":"keyword"},"resource_provider":{"ignore_above":10000,"type":"keyword"},"resource_uri":{"ignore_above":10000,"type":"keyword"},"response_code":{"type":"long"},"response_content_type":{"ignore_above":10000,"type":"keyword"},"return_value":{"ignore_above":10000,"type":"keyword"},"rule_id":{"ignore_above":10000,"type":"keyword"},"rule_uuid":{"ignore_above":10000,"type":"keyword"},"security_group_id":{"ignore_above":10000,"type":"keyword"},"security_group_name":{"ignore_above":10000,"type":"keyword"},"sensor_event_rate":{"type":"double"},"sensor_name":{"ignore_above":10000,"type":"keyword"},"sensor_uuid":{"ignore_above":10000,"type":"keyword"},"session":{"ignore_above":10000,"type":"keyword"},"shared_resource_name":{"ignore_above":10000,"type":"keyword"},"short_message":{"ignore_above":10000,"type":"keyword"},"silent":{"type":"boolean"},"source_account":{"ignore_above":10000,"type":"keyword"},"source_account_id":{"ignore_above":10000,"type":"keyword"},"source_account_name":{"ignore_above":10000,"type":"keyword"},"source_additional_hostnames":{"ignore_above":10000,"type":"keyword"},"source_address":{"ignore_above":10000,"type":"keyword"},"source_address_6":{"ignore_above":10000,"type":"keyword"},"source_asn":{"ignore_above":10000,"type":"keyword"},"source_asset_id":{"ignore_above":10000,"type":"keyword"},"source_blacklist_activity":{"ignore_above":10000,"type":"keyword"},"source_blacklist_priority":{"ignore_above":10000,"type":"keyword"},"source_blacklist_reliability":{"ignore_above":10000,"type":"keyword"},"source_canonical":{"ignore_above":10000,"type":"keyword"},"source_city":{"ignore_above":10000,"type":"keyword"},"source_country":{"ignore_above":10000,"type":"keyword"},"source_cpe":{"ignore_above":10000,"type":"keyword"},"source_datacenter":{"ignore_above":10000,"type":"keyword"},"source_datastore":{"ignore_above":10000,"type":"keyword"},"source_dns_domain":{"ignore_above":10000,"type":"keyword"},"source_fqdn":{"ignore_above":10000,"type":"keyword"},"source_hostname":{"ignore_above":10000,"type":"keyword"},"source_infrastructure_name":{"ignore_above":10000,"type":"keyword"},"source_infrastructure_type":{"ignore_above":10000,"type":"keyword"},"source_instance_id":{"ignore_above":10000,"type":"keyword"},"source_latitude":{"ignore_above":10000,"type":"keyword"},"source_location_id":{"ignore_above":10000,"type":"keyword"},"source_location_name":{"ignore_above":10000,"type":"keyword"},"source_longitude":{"ignore_above":10000,"type":"keyword"},"source_mac":{"ignore_above":10000,"type":"keyword"},"source_mac_vendor":{"ignore_above":10000,"type":"keyword"},"source_name":{"ignore_above":10000,"type":"keyword"},"source_nat_address":{"ignore_above":10000,"type":"keyword"},"source_nat_port":{"type":"long"},"source_netmask":{"ignore_above":10000,"type":"keyword"},"source_network":{"ignore_above":10000,"type":"keyword"},"source_ntdomain":{"ignore_above":10000,"type":"keyword"},"source_organisation":{"ignore_above":10000,"type":"keyword"},"source_port":{"type":"long"},"source_port_label":{"ignore_above":10000,"type":"keyword"},"source_post_nat_port":{"type":"long"},"source_pre_nat_port":{"type":"long"},"source_process":{"ignore_above":10000,"type":"keyword"},"source_process_commandline":{"ignore_above":10000,"type":"keyword"},"source_process_id":{"ignore_above":10000,"type":"keyword"},"source_process_parent":{"ignore_above":10000,"type":"keyword"},"source_process_parent_commandline":{"ignore_above":10000,"type":"keyword"},"source_process_parent_process_id":{"ignore_above":10000,"type":"keyword"},"source_region":{"ignore_above":10000,"type":"keyword"},"source_registered_country":{"ignore_above":10000,"type":"keyword"},"source_service_name":{"ignore_above":10000,"type":"keyword"},"source_translated_address":{"ignore_above":10000,"type":"keyword"},"source_translated_port":{"type":"long"},"source_user_email":{"ignore_above":10000,"type":"keyword"},"source_user_email_domain":{"ignore_above":10000,"type":"keyword"},"source_user_group":{"ignore_above":10000,"type":"keyword"},"source_user_id":{"ignore_above":10000,"type":"keyword"},"source_user_privileges":{"ignore_above":10000,"type":"keyword"},"source_userid":{"ignore_above":10000,"type":"keyword"},"source_username":{"ignore_above":10000,"type":"keyword"},"source_vhost":{"ignore_above":10000,"type":"keyword"},"source_vpc":{"ignore_above":10000,"type":"keyword"},"source_vpn":{"ignore_above":10000,"type":"keyword"},"source_workstation":{"ignore_above":10000,"type":"keyword"},"source_zone":{"ignore_above":10000,"type":"keyword"},"ssh_authorized_key":{"ignore_above":10000,"type":"keyword"},"ssh_client_proto":{"ignore_above":10000,"type":"keyword"},"ssh_client_software":{"ignore_above":10000,"type":"keyword"},"ssh_server_proto":{"ignore_above":10000,"type":"keyword"},"ssh_server_software":{"ignore_above":10000,"type":"keyword"},"stat_value":{"type":"long"},"status":{"ignore_above":10000,"type":"keyword"},"suppress_rule_id":{"ignore_above":10000,"type":"keyword"},"suppress_rule_name":{"ignore_above":10000,"type":"keyword"},"suppressed":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"syslog_source":{"ignore_above":10000,"type":"keyword"},"tag":{"ignore_above":10000,"type":"keyword"},"threat_intelligence_feed_name":{"ignore_above":10000,"type":"keyword"},"threat_intelligence_matched_metadata":{"ignore_above":10000,"type":"keyword"},"ticket_encryption_type":{"ignore_above":10000,"type":"keyword"},"timeStamp":{"ignore_above":10000,"type":"keyword"},"time_end":{"type":"date"},"time_offset":{"ignore_above":10000,"type":"keyword"},"time_start":{"type":"date"},"time_zone":{"ignore_above":10000,"type":"keyword"},"timestamp_arrived":{"type":"date"},"timestamp_end":{"type":"date"},"timestamp_occured":{"type":"date"},"timestamp_occured_iso8601":{"type":"date"},"timestamp_os":{"type":"date"},"timestamp_received":{"type":"date"},"timestamp_received_iso8601":{"type":"date"},"timestamp_start":{"type":"date"},"timestamp_to_storage":{"type":"date"},"tls_cipher":{"ignore_above":10000,"type":"keyword"},"tls_fingerprint":{"ignore_above":10000,"type":"keyword"},"tls_issuerdn":{"ignore_above":10000,"type":"keyword"},"tls_sni":{"ignore_above":10000,"type":"keyword"},"tls_subject":{"ignore_above":10000,"type":"keyword"},"tls_version":{"ignore_above":10000,"type":"keyword"},"total_disconnection_time":{"ignore_above":10000,"type":"keyword"},"total_packets":{"type":"long"},"transaction_status":{"ignore_above":10000,"type":"keyword"},"transient":{"type":"boolean"},"transport_protocol":{"ignore_above":10000,"type":"keyword"},"tty_terminal":{"ignore_above":10000,"type":"keyword"},"used_hint":{"type":"boolean"},"user_group_id":{"ignore_above":10000,"type":"keyword"},"user_policy":{"ignore_above":10000,"type":"keyword"},"user_realm":{"ignore_above":10000,"type":"keyword"},"user_resource":{"ignore_above":10000,"type":"keyword"},"user_resource_type":{"ignore_above":10000,"type":"keyword"},"user_role":{"ignore_above":10000,"type":"keyword"},"user_type":{"ignore_above":10000,"type":"keyword"},"uuid":{"ignore_above":10000,"type":"keyword"},"virtual_source_address":{"ignore_above":10000,"type":"keyword"},"virtual_source_name":{"ignore_above":10000,"type":"keyword"},"was_fuzzied":{"type":"boolean"},"was_guessed":{"type":"boolean"},"watchlist":{"ignore_above":10000,"type":"keyword"},"wireless_ap":{"ignore_above":10000,"type":"keyword"},"wireless_bssid":{"ignore_above":10000,"type":"keyword"},"wireless_channel":{"ignore_above":10000,"type":"keyword"},"wireless_encryption":{"ignore_above":10000,"type":"keyword"},"wireless_ssid":{"ignore_above":10000,"type":"keyword"},"x_att_tenantid":{"ignore_above":10000,"type":"keyword"}}},"timeStamp":{"ignore_above":10000,"type":"keyword"}}""")
        insertSampleBulkData(index, javaClass.classLoader.getResource("data/message_logs.ndjson").readText())
    }

    @Suppress("UNCHECKED_CAST")
    protected fun extractFailuresFromSearchResponse(searchResponse: Response): List<Map<String, String>?>? {
        val shards = searchResponse.asMap()["_shards"] as Map<String, ArrayList<Map<String, Any>>>
        assertNotNull(shards)
        val failures = shards["failures"]
        assertNotNull(failures)
        return failures?.let {
            val result: ArrayList<Map<String, String>?>? = ArrayList()
            for (failure in it) {
                result?.add((failure as Map<String, Map<String, String>>)["reason"])
            }
            return result
        }
    }

    override fun preserveIndicesUponCompletion(): Boolean = true
    companion object {
        @JvmStatic
        val isMultiNode = System.getProperty("cluster.number_of_nodes", "1").toInt() > 1
        protected val defaultKeepIndexSet = setOf(".opendistro_security")
        /**
         * We override preserveIndicesUponCompletion to true and use this function to clean up indices
         * Meant to be used in @After or @AfterClass of your feature test suite
         */
        fun wipeAllIndices(client: RestClient = adminClient(), keepIndex: kotlin.collections.Set<String> = defaultKeepIndexSet) {
            try {
                client.performRequest(Request("DELETE", "_data_stream/*"))
            } catch (e: ResponseException) {
                // We hit a version of ES that doesn't serialize DeleteDataStreamAction.Request#wildcardExpressionsOriginallySpecified field or
                // that doesn't support data streams so it's safe to ignore
                val statusCode = e.response.statusLine.statusCode
                if (!setOf(404, 405, 500).contains(statusCode)) {
                    throw e
                }
            }

            val response = client.performRequest(Request("GET", "/_cat/indices?format=json&expand_wildcards=all"))
            val xContentType = XContentType.fromMediaType(response.entity.contentType.value)
            xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.entity.content
            ).use { parser ->
                for (index in parser.list()) {
                    val jsonObject: Map<*, *> = index as java.util.HashMap<*, *>
                    val indexName: String = jsonObject["index"] as String
                    // .opendistro_security isn't allowed to delete from cluster
                    if (!keepIndex.contains(indexName)) {
                        val request = Request("DELETE", "/$indexName")
                        // TODO: remove PERMISSIVE option after moving system index access to REST API call
                        val options = RequestOptions.DEFAULT.toBuilder()
                        options.setWarningsHandler(WarningsHandler.PERMISSIVE)
                        request.options = options.build()
                        client.performRequest(request)
                    }
                }
            }

            waitFor {
                if (!isMultiNode) {
                    waitForRunningTasks(client)
                    waitForPendingTasks(client)
                    waitForThreadPools(client)
                } else {
                    // Multi node test is not suitable to waitFor
                    // We have seen long-running write task that fails the waitFor
                    //  probably because of cluster manager - data node task not in sync
                    // So instead we just sleep 1s after wiping indices
                    Thread.sleep(1_000)
                }
            }
        }

        @JvmStatic
        @Throws(IOException::class)
        protected fun waitForRunningTasks(client: RestClient) {
            val runningTasks: MutableSet<String> = runningTasks(client.performRequest(Request("GET", "/_tasks?detailed")))
            if (runningTasks.isEmpty()) {
                return
            }
            val stillRunning = ArrayList<String>(runningTasks)
            fail("${Date()}: There are still tasks running after this test that might break subsequent tests: \n${stillRunning.joinToString("\n")}.")
        }

        @Suppress("UNCHECKED_CAST")
        @Throws(IOException::class)
        private fun runningTasks(response: Response): MutableSet<String> {
            val runningTasks: MutableSet<String> = HashSet()
            val nodes = entityAsMap(response)["nodes"] as Map<String, Any>?
            for ((_, value) in nodes!!) {
                val nodeInfo = value as Map<String, Any>
                val nodeTasks = nodeInfo["tasks"] as Map<String, Any>?
                for ((_, value1) in nodeTasks!!) {
                    val task = value1 as Map<String, Any>
                    // Ignore the task list API - it doesn't count against us
                    if (task["action"] == ListTasksAction.NAME || task["action"] == ListTasksAction.NAME + "[n]") continue
                    // runningTasks.add(task["action"].toString() + " | " + task["description"].toString())
                    runningTasks.add(task.toString())
                }
            }
            return runningTasks
        }

        @JvmStatic
        protected fun waitForThreadPools(client: RestClient) {
            val response = client.performRequest(Request("GET", "/_cat/thread_pool?format=json"))

            val xContentType = XContentType.fromMediaType(response.entity.contentType.value)
            xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.entity.content
            ).use { parser ->
                for (index in parser.list()) {
                    val jsonObject: Map<*, *> = index as java.util.HashMap<*, *>
                    val active = (jsonObject["active"] as String).toInt()
                    val queue = (jsonObject["queue"] as String).toInt()
                    val name = jsonObject["name"]
                    val trueActive = if (name == "management") active - 1 else active
                    if (trueActive > 0 || queue > 0) {
                        fail("Still active threadpools in cluster: $jsonObject")
                    }
                }
            }
        }

        internal interface IProxy {
            val version: String?
            var sessionId: String?

            fun getExecutionData(reset: Boolean): ByteArray?
            fun dump(reset: Boolean)
            fun reset()
        }

        /*
        * We need to be able to dump the jacoco coverage before the cluster shuts down.
        * The new internal testing framework removed some gradle tasks we were listening to,
        * to choose a good time to do it. This will dump the executionData to file after each test.
        * TODO: This is also currently just overwriting integTest.exec with the updated execData without
        *   resetting after writing each time. This can be improved to either write an exec file per test
        *   or by letting jacoco append to the file.
        * */
        @JvmStatic
        @AfterClass
        fun dumpCoverage() {
            // jacoco.dir set in esplugin-coverage.gradle, if it doesn't exist we don't
            // want to collect coverage, so we can return early
            val jacocoBuildPath = System.getProperty("jacoco.dir") ?: return
            val serverUrl = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7777/jmxrmi"
            JMXConnectorFactory.connect(JMXServiceURL(serverUrl)).use { connector ->
                val proxy = MBeanServerInvocationHandler.newProxyInstance(
                    connector.mBeanServerConnection,
                    ObjectName("org.jacoco:type=Runtime"),
                    IProxy::class.java,
                    false
                )
                proxy.getExecutionData(false)?.let {
                    val path = PathUtils.get("$jacocoBuildPath/integTest.exec")
                    Files.write(path, it)
                }
            }
        }
    }
}
