package com.example.pocconcurrency;

import com.mongodb.client.result.UpdateResult;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class Worker {

    private final MongoTemplate mongoTemplate;

    private final AtomicLong count = new AtomicLong(0L);

    @Autowired
    public Worker(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Scheduled(initialDelay = 100)
    @Scheduled(initialDelay = 150)
    @Scheduled(initialDelay = 300)
    private void processBulk() throws InterruptedException {

        LocalDateTime now = LocalDateTime.now();
        long timeoutMin = 5; // 5 minutes
        int limit = 10;

        Query query = new Query(Criteria.where("status").is("pending"));
        query.limit(limit);
        query.fields().include("_id");
        query.with(Sort.by(Sort.Direction.ASC, "_id"));

        List<Person> unlocked;
        do {
            // Fetch unlocked documents
            unlocked = mongoTemplate.find(query, Person.class);
            List<ObjectId> idsToLock = unlocked.stream().map(Person::getId).toList();
            if (idsToLock.isEmpty()) {
                continue;
            }

            // Mark locked documents as processing
            Query updateQuery = new Query(new Criteria().andOperator(
                    Criteria.where("status").is("pending"),
                    Criteria.where("_id").in(idsToLock),
                    new Criteria().orOperator(
                            Criteria.where("lockedAt").is(null),
                            Criteria.where("lockedAt").lt(now.minusMinutes(timeoutMin))
                    )
            ));
            Update update = new Update()
                    .set("status", "processing")
                    .set("lockedAt", now);

            UpdateResult updateResult = mongoTemplate.updateMulti(updateQuery, update, Person.class);

            if (updateResult.getModifiedCount() == 0) {
                continue;
            }

            //Do some processing
            log.info("Processing {} persons", updateResult.getModifiedCount());
            count.addAndGet(updateResult.getModifiedCount());

            Query toProcessQuery = new Query(Criteria.where("status").is("processing")
                    .andOperator(Criteria.where("_id").in(idsToLock)));
            List<Person> toProcess = mongoTemplate.find(toProcessQuery, Person.class);

            Thread.sleep(new Random().nextInt(1, 1000)); // Simulate some processing time
            toProcess.forEach(person -> log.info(person.toString()));

            // Mark processed documents as done
            markAsProcessedBulk(idsToLock);

        } while (!unlocked.isEmpty());

        log.info("Total: {}", count.intValue());
    }

    //    @Scheduled(fixedDelay = 1000, initialDelay = 100)
//    @Scheduled(fixedDelay = 1000, initialDelay = 100)
//    @Scheduled(fixedDelay = 1000, initialDelay = 100)
    private void process() {

        final AtomicLong count = new AtomicLong(0L);

        boolean hasPending = true;

        while (hasPending) {
            Query query = new Query();
            LocalDateTime now = LocalDateTime.now();
            long timeoutMin = 5; // 5 minutes

            query.addCriteria(new Criteria().andOperator(
                    Criteria.where("status").is("pending"),
                    new Criteria().orOperator(
                            Criteria.where("lockedAt").is(null),
                            Criteria.where("lockedAt").lt(now.minusMinutes(timeoutMin))
                    )
            ));
            query.with(Sort.by(Sort.Direction.ASC, "_id"));

            Update update = new Update()
                    .set("status", "processing")
                    .set("lockedAt", now);

            FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);

            /* *
             * The read and update happen together as a single atomic operation.
             * No other thread or client can interleave between the read and the update for the same document.
             * This is safe across multiple threads and processes.
             * */
            Person person = mongoTemplate.findAndModify(query, update, options, Person.class);

            if (person == null) {
                hasPending = false;
            } else {
                count.incrementAndGet();

                log.info("Reading person: {}", person);

                markAsProcessed(person);
            }
        }

        log.info("Total: {}", count.intValue());
    }

    private void markAsProcessedBulk(List<ObjectId> idsToMarkAsDone) {
        Query updateQuery = new Query(new Criteria().andOperator(
                Criteria.where("_id").in(idsToMarkAsDone)
        ));
        Update update = new Update()
                .set("status", "done")
                .unset("lockedAt");
        mongoTemplate.updateMulti(updateQuery, update, Person.class);
    }

    private void markAsProcessed(Person person) {
        Query query = Query.query(Criteria.where("_id").is(person.getId()));
        Update update = new Update()
                .set("status", "done")
                .unset("lockedAt");
        mongoTemplate.updateFirst(query, update, Person.class);
    }
}
