package com.myorg.repository;

import com.myorg.document.FailedLog;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface FailedLogRepository  extends MongoRepository <FailedLog, String> {
}
