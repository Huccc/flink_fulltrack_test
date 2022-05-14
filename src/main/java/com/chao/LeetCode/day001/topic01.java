package com.chao.LeetCode.day001;

import java.util.Arrays;
import java.util.HashMap;

public class topic01 {
	public static void main(String[] args) {
		Solution solution = new Solution();
//		System.out.println(solution.twoSum(new int[]{1, 2, 3, 4}, 5));
		System.out.println(Arrays.toString(solution.twoSum3(new int[]{1, 2, 3, 6}, 7)));
	}
}

class Solution {
	public int[] twoSum3(int[] nums, int target) {
		HashMap<Integer, Integer> hashMap = new HashMap<>();
		for (int i = 0; i < nums.length; i++) {
			if (hashMap.containsKey(target - nums[i])) {
				return new int[]{hashMap.get(target - nums[i]), i};
			}
			hashMap.put(nums[i], i);
		}
		return new int[1];
	}
	
	
	public int[] twoSum(int[] nums, int target) {
		for (int i = 0; i < nums.length; i++) {
			for (int j = i + 1; j < nums.length; j++) {
				if (nums[i] + nums[j] == target) {
					return new int[]{i, j};
				}
			}
		}
		return new int[0];
	}
	
	public int[] twoSum2(int[] nums, int target) {
		HashMap<Integer, Integer> hashMap = new HashMap<>();
		for (int i = 0; i < nums.length; i++) {
			if (hashMap.containsKey(target - nums[i])) {
				return new int[]{hashMap.get(target - nums[i]), i};
			}
			hashMap.put(nums[i], i);
		}
		return new int[2];
	}
}
