package com.myorg.repository;

import com.myorg.document.LogDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface LogRepository extends MongoRepository <LogDocument , String> {
}
