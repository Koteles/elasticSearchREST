package com.pluralsight.repository;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.inject.Inject;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.google.gson.Gson;
import com.model.Student;

public class StudentRepositoryStub implements StudentRepository {

	private Gson gson = new Gson();

	private static final Client client;

	static {
		Properties p = new Properties();

		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		InputStream input = null;

		Client template = null;
		try {
			input = loader.getResourceAsStream("config.properties");

			p.load(input);
			String host = p.getProperty("elasticSearchHost").trim();

			String port = p.getProperty("elasticSearchPort").trim();

			int portNumber = Integer.parseInt(port);
			template = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), portNumber));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client = template;
	}

	public List<Student> findAllStudents(String index, String type) {

		int scrollSize = 1000;

		SearchResponse response = null;

		int i = 0;

		// List<String> esData = new ArrayList<String>();

		List<Student> students = new ArrayList<Student>();

		SearchRequestBuilder addSort = client.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery()).setSize(scrollSize).addSort("id", SortOrder.ASC);

		while (response == null || response.getHits().hits().length != 0) {

			// esData.clear();

			response = addSort.setFrom(i * scrollSize).get();

			for (SearchHit hit : response.getHits()) {

				// esData.add();
				Student student = gson.fromJson(hit.getSourceAsString(), Student.class);

				students.add(student);

			}

			i++;

			// for (String source : esData) {

		}

		return students;

	}

	public Student findStudent(String index, String type, String studentId) {

		GetResponse response = client.prepareGet(index, type, studentId).get();

		String search = response.getSourceAsString();

		Student student = gson.fromJson(search, Student.class);

		return student;

	}

	public void addStudent(Student student, String index, String type) {

		String id = student.getId();
		String json = gson.toJson(student, Student.class);

		client.prepareIndex(index, type, id).setSource(json).get();
	}

	public void deleteStudent(String index, String type, String studentId) {

		client.prepareDelete(index, type, studentId).get();

	}
}
