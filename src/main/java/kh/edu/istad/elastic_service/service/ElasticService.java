package kh.edu.istad.elastic_service.service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kh.edu.istad.elastic_service.domain.CommunityEngagement;
import kh.edu.istad.elastic_service.domain.Content;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticService {

    private final ElasticsearchOperations elasticsearchOperations;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "content-service.contents", groupId = "content-group")
    public void handleContentChange(String message, Acknowledgment acknowledgment) {
        try {
            // Log the raw message for debugging
            log.debug("Received raw Kafka message: {}", message);

            // First, handle potential BOM and encoding issues
            message = cleanMessage(message);

            // Log the cleaned message
            log.debug("Cleaned Kafka message: {}", message);

            // Parse the JSON message
            Map<String, Object> messageMap = parseMessage(message);
            if (messageMap == null) {
                log.error("Failed to parse message as JSON: {}", message);
                acknowledgment.acknowledge();
                return;
            }

            // Process the message based on operation type
            processMessage(messageMap);

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing Kafka message: {}", e.getMessage(), e);
            if (isUnrecoverableError(e)) {
                log.warn("Acknowledging message due to unrecoverable error");
                acknowledgment.acknowledge();
            }
        }
    }

    private String cleanMessage(String message) {
        if (message == null) {
            return null;
        }

        // Remove BOM if present
        if (message.startsWith("\uFEFF")) {
            message = message.substring(1);
        }

        // Remove leading 'z' character if present
        if (message.startsWith("z{")) {
            message = message.substring(1);
        }

        // Remove any non-printable characters except valid JSON characters
        message = message.replaceAll("[^\\x20-\\x7E]", "");

        // Ensure proper UTF-8 encoding
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        String cleaned = new String(bytes, StandardCharsets.UTF_8);

        // Verify the message starts with a valid JSON character
        if (!cleaned.startsWith("{") && !cleaned.startsWith("[")) {
            // Find the first occurrence of { or [
            int jsonStart = Math.min(
                    cleaned.indexOf("{") != -1 ? cleaned.indexOf("{") : Integer.MAX_VALUE,
                    cleaned.indexOf("[") != -1 ? cleaned.indexOf("[") : Integer.MAX_VALUE
            );

            if (jsonStart != Integer.MAX_VALUE) {
                cleaned = cleaned.substring(jsonStart);
            }
        }

        return cleaned;
    }

    private Map<String, Object> parseMessage(String message) {
        try {
            return objectMapper.readValue(message, Map.class);
        } catch (Exception e) {
            log.error("JSON parsing error: {}", e.getMessage());
            return null;
        }
    }

    private void processMessage(Map<String, Object> messageMap) {
        String operationType = (String) messageMap.get("operationType");
        if (operationType == null) {
            log.error("Operation type is missing in message");
            return;
        }

        try {
            switch (operationType) {
                case "insert":
                    handleInsertOperation(messageMap);
                    break;
                case "update":
                    handleUpdateOperation(messageMap);
                    break;
                case "delete":
                    handleDeleteOperation(messageMap);
                    break;
                default:
                    log.warn("Unsupported operationType: {}", operationType);
            }
        } catch (Exception e) {
            log.error("Error processing {} operation: {}", operationType, e.getMessage(), e);
            throw e;
        }
    }

    private void handleInsertOperation(Map<String, Object> messageMap) {
        // Extract the fullDocument part from the message
        Map<String, Object> fullDocument = (Map<String, Object>) messageMap.get("fullDocument");
        if (fullDocument == null) {
            log.error("Full document is missing in insert operation");
            return;
        }

        // Map the MongoDB document to the Elasticsearch Content model
        Content content = mapToContent(fullDocument);

        // Save the content to Elasticsearch
        elasticsearchOperations.save(content);
        log.info("Content inserted into Elasticsearch: {}", content.getId());
    }

    private void handleUpdateOperation(Map<String, Object> messageMap) {
        // Extract the fullDocument part from the message
        Map<String, Object> fullDocument = (Map<String, Object>) messageMap.get("fullDocument");
        if (fullDocument == null) {
            log.error("Full document is missing in update operation");
            return;
        }

        // Map the MongoDB document to the Elasticsearch Content model
        Content content = mapToContent(fullDocument);

        // Update the content in Elasticsearch
        elasticsearchOperations.save(content);
        log.info("Content updated in Elasticsearch: {}", content.getId());
    }

    private void handleDeleteOperation(Map<String, Object> messageMap) {
        // Extract the documentKey part from the message
        Map<String, Object> documentKey = (Map<String, Object>) messageMap.get("documentKey");
        if (documentKey == null) {
            log.error("Document key is missing in delete operation");
            return;
        }

        Map<String, String> idMap = (Map<String, String>) documentKey.get("_id");
        if (idMap == null || !idMap.containsKey("$oid")) {
            log.error("Invalid document key format");
            return;
        }

        String id = idMap.get("$oid");
        if (id == null) {
            log.error("Document ID is missing in delete operation");
            return;
        }

        // Delete the content from Elasticsearch
        elasticsearchOperations.delete(id, Content.class);
        log.info("Content deleted from Elasticsearch: {}", id);
    }

    private Content mapToContent(Map<String, Object> fullDocument) {
        Content content = new Content();

        // Handle MongoDB _id format
        Map<String, String> idMap = (Map<String, String>) fullDocument.get("_id");
        if (idMap != null && idMap.containsKey("$oid")) {
            content.setId(idMap.get("$oid"));
        } else {
            content.setId(fullDocument.get("_id").toString());
        }

        // Set basic fields
        content.setTitle(getStringValue(fullDocument, "title"));
        content.setContent(getStringValue(fullDocument, "content"));
        content.setThumbnail(getStringValue(fullDocument, "thumbnail"));
        content.setSlug(getStringValue(fullDocument, "slug"));
        content.setTags((List<String>) fullDocument.get("tags"));

        // Map communityEngagement
        Map<String, Object> engagementMap = (Map<String, Object>) fullDocument.get("communityEngagement");
        if (engagementMap != null) {
            CommunityEngagement engagement = new CommunityEngagement();
            engagement.setLikeCount(parseNumber(engagementMap.get("likeCount")));
            engagement.setCommentCount(parseNumber(engagementMap.get("commentCount")));
            engagement.setReportCount(parseNumber(engagementMap.get("reportCount")));
            engagement.setFireCount(parseNumber(engagementMap.get("fireCount")));
            engagement.setLoveCount(parseNumber(engagementMap.get("loveCount")));
            engagement.setLastUpdated(parseDate(engagementMap.get("lastUpdated")));
            content.setCommunityEngagement(engagement);
        }

        // Set boolean fields with default values if null
        content.setIsDeleted(getBooleanValue(fullDocument, "isDeleted", false));
        content.setIsDraft(getBooleanValue(fullDocument, "isDraft", false));
        content.setAuthorUuid(getStringValue(fullDocument, "author_uuid"));

        // Map dates
        content.setCreatedDate(parseDate(fullDocument.get("created_date")));
        content.setLastModifiedDate(parseDate(fullDocument.get("last_modified_date")));

        return content;
    }

    private String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Map) {
            Map<String, Object> valueMap = (Map<String, Object>) value;
            if (valueMap.containsKey("$oid")) {
                return (String) valueMap.get("$oid");
            }
        }
        return value != null ? value.toString() : null;
    }

    private Boolean getBooleanValue(Map<String, Object> map, String key, Boolean defaultValue) {
        Object value = map.get(key);
        return value != null ? (Boolean) value : defaultValue;
    }

    private Long parseNumber(Object numberObject) {
        if (numberObject == null) {
            return 0L;
        }

        if (numberObject instanceof Map) {
            // Handle MongoDB's $numberLong format
            Map<String, String> numberMap = (Map<String, String>) numberObject;
            return Long.parseLong(numberMap.get("$numberLong"));
        } else if (numberObject instanceof Number) {
            // Handle direct numeric values
            return ((Number) numberObject).longValue();
        } else if (numberObject instanceof String) {
            // Handle string numbers
            try {
                return Long.parseLong((String) numberObject);
            } catch (NumberFormatException e) {
                log.warn("Failed to parse number from string: {}", numberObject);
                return 0L;
            }
        }
        return 0L;
    }

    private LocalDateTime parseDate(Object dateObject) {
        if (dateObject == null) {
            return null;
        }

        try {
            if (dateObject instanceof Map) {
                // Handle MongoDB's $date format
                Map<String, Object> dateMap = (Map<String, Object>) dateObject;
                Object dateValue = dateMap.get("$date");

                if (dateValue instanceof Long) {
                    // Handle timestamp in milliseconds
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) dateValue),
                            ZoneId.systemDefault());
                } else if (dateValue instanceof String) {
                    // Handle ISO date string
                    return LocalDateTime.parse((String) dateValue,
                            DateTimeFormatter.ISO_DATE_TIME);
                }
            } else if (dateObject instanceof String) {
                // Handle direct date strings
                return LocalDateTime.parse((String) dateObject,
                        DateTimeFormatter.ISO_DATE_TIME);
            } else if (dateObject instanceof Long) {
                // Handle direct timestamp in milliseconds
                return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) dateObject),
                        ZoneId.systemDefault());
            }
        } catch (Exception e) {
            log.error("Error parsing date: {}", dateObject, e);
        }
        return null;
    }

    private boolean isUnrecoverableError(Exception e) {
        return e instanceof JsonParseException ||
                e.getMessage().contains("Malformed JSON") ||
                e.getMessage().contains("Invalid UTF-8");
    }
}