/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestIndexTransformActionIT : TransformRestTestCase() {
    // TODO: Once GET API written, add update transform tests (required for createTransform helper)

    @Throws(Exception::class)
    fun `test creating a transform`() {
        val transform = randomTransform()
        createTransformSourceIndex(transform)
        val response = client().makeRequest(
            "PUT",
            "$TRANSFORM_BASE_URI/${transform.id}",
            emptyMap(),
            transform.toHttpEntity()
        )
        assertEquals("Create transform failed", RestStatus.CREATED, response.restStatus())
        val responseBody = response.asMap()
        val createdId = responseBody["_id"] as String
        assertNotEquals("Response is missing Id", Transform.NO_ID, createdId)
        assertEquals("Not same id", transform.id, createdId)
        assertEquals("Incorrect Location header", "$TRANSFORM_BASE_URI/$createdId", response.getHeader("Location"))
    }

    @Throws(Exception::class)
    fun `test creating a transform with no id fails`() {
        try {
            val transform = randomTransform()
            client().makeRequest(
                "PUT",
                TRANSFORM_BASE_URI,
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test creating a transform with POST fails`() {
        try {
            val transform = randomTransform()
            client().makeRequest(
                "POST",
                "$TRANSFORM_BASE_URI/some_transform",
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test mappings after transform creation`() {
        createRandomTransform()

        val response = client().makeRequest("GET", "/$INDEX_MANAGEMENT_INDEX/_mapping")
        val parserMap = createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[INDEX_MANAGEMENT_INDEX]!!["mappings"] as Map<String, Any>
        val expected = createParser(
            XContentType.JSON.xContent(),
            javaClass.classLoader.getResource("mappings/opendistro-ism-config.json")
                .readText()
        )
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }

    @Throws(Exception::class)
    fun `test updating transform continuous field fails`() {
        val transform = createRandomTransform()
        try {
            client().makeRequest(
                "PUT",
                "$TRANSFORM_BASE_URI/${transform.id}?refresh=true&if_seq_no=${transform.seqNo}&if_primary_term=${transform.primaryTerm}",
                emptyMap(),
                transform.copy(continuous = !transform.continuous, pageSize = 50).toHttpEntity() // Lower page size to make sure that doesn't throw an error first
            )
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "status_exception", "reason" to "Not allowed to modify [continuous]")
                    ),
                    "type" to "status_exception",
                    "reason" to "Not allowed to modify [continuous]"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }
}
