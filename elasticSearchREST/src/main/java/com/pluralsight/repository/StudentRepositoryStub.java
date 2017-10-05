package com.pluralsight.repository;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.elasticsearch.action.get.GetResponse;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.google.gson.Gson;
import com.pluralsight.model.Student;

public class StudentRepositoryStub implements StudentRepository {

	private Gson gson = new Gson();
	
/*	@Inject
	private Client client;*/
	
	public List<Student> findAllStudents(String index, String type) {

		Client client = null;
		try {
			client = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		}

		catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int scrollSize = 1000;

		SearchResponse response = null;

		int i = 0;

		List<String> esData = new ArrayList<String>();

		List<Student> students = new ArrayList<Student>();

		while (response == null || response.getHits().hits().length != 0) {

			esData.clear();

			response = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery())

					.setSize(scrollSize).addSort("id", SortOrder.ASC).setFrom(i * scrollSize).get();

			for (SearchHit hit : response.getHits()) {

				esData.add(hit.getSourceAsString());

			}

			i++;

			for (String source : esData) {

				Student student = gson.fromJson(source, Student.class);

				students.add(student);

			}

		}

		return students;

	}

	public Student findStudent(String index, String type, String studentId) {

		Client client = null;

		try {
			client = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		GetResponse response = client.prepareGet(index, type, studentId).get();

		String search = response.getSourceAsString();

		Student student = gson.fromJson(search, Student.class);

		return student;

	}

	public void addStudent(Student student, String index, String type) {
		
		String id = Integer.toString(student.getId());
		String json = gson.toJson(student, Student.class);
		Client client = null;
		try {
			client = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		client.prepareIndex(index, type, id).setSource(json).get();
	}
	
	public void deleteStudent(String index, String type, String studentId) {

		Client client = null;

		try {
			client = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		client.prepareDelete(index, type, studentId).get();


	}
}
