package com.tair.cli.util;

import static junit.framework.TestCase.assertEquals;

import java.util.Iterator;

import org.junit.Test;

/**
 * @author Baoyi Chen
 */
public class IteratorsTest {
	@Test
	public void test() {
		Iterator<Integer> it = Iterators.iterator(1, 2, 3);
		assertEquals(1, it.next().intValue());
		assertEquals(2, it.next().intValue());
		assertEquals(3, it.next().intValue());
	}
}