package com.elasticsearch.apachekafkaelastic.repo;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.elasticsearch.apachekafkaelastic.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Repository
public class ElasticSearchQuery {


    @Autowired
    private ElasticsearchClient elasticsearchClient;

    private final String indexName = "product";


    public String createOrUpdateDocument(Product Product) throws IOException {

        IndexResponse response = elasticsearchClient.index(i -> i
                        .index(indexName)
//                        .id(Product.getId())
                        .document(Product)
                                                          );
        if (response.result()
                .name()
                .equals("Created")) {
            return new StringBuilder("Document has been successfully created.").toString();
        } else if (response.result()
                .name()
                .equals("Updated")) {
            return new StringBuilder("Document has been successfully updated.").toString();
        }
        return new StringBuilder("Error while performing the operation.").toString();
    }

    public Product getDocumentById(String DatamodelId) throws IOException {
        Product Product = null;
        GetResponse<Product> response = elasticsearchClient.get(g -> g
                        .index(indexName)
                        .id(DatamodelId),
                Product.class
                                                               );

        if (response.found()) {
            Product = response.source();
            System.out.println("Datamodel name " + Product.getName());
        } else {
            System.out.println("Datamodel not found");
        }

        return Product;
    }

    public String deleteDocumentById(String DatamodelId) throws IOException {

        DeleteRequest request = DeleteRequest.of(d -> d.index(indexName)
                .id(DatamodelId));

        DeleteResponse deleteResponse = elasticsearchClient.delete(request);
        if (Objects.nonNull(deleteResponse.result()) && !deleteResponse.result()
                .name()
                .equals("NotFound")) {
            return new StringBuilder("Datamodel with id " + deleteResponse.id() + " has been deleted.").toString();
        }
        System.out.println("Datamodel not found");
        return new StringBuilder("Datamodel with id " + deleteResponse.id() + " does not exist.").toString();

    }

    public List<Product> searchAllDocuments( ) throws IOException {

        SearchRequest searchRequest = SearchRequest.of(s -> s.index(indexName));
        SearchResponse searchResponse = elasticsearchClient.search(searchRequest, Product.class);
        List<Hit> hits = searchResponse.hits()
                .hits();
        List<Product> products = new ArrayList<>();
        for (Hit object : hits) {

            System.out.print(((Product) object.source()));
            products.add((Product) object.source());

        }
        return products;
    }
}