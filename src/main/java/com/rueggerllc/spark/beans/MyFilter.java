package com.rueggerllc.spark.beans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

public class MyFilter {
	
	public static <T> Collection<T> filter(Predicate<T> predicate, Collection<T> items) {
		Collection<T> result = new ArrayList<T>();
		for(T item: items) {
			if (predicate.test(item)) {
				result.add(item);
			}
		}
		return result;
	}	
	
	
	
}
