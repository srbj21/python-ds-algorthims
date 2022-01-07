# Given an array, rotate the array to the right by k steps, where k is non-negative.
# Example 1:
# Input: nums = [1,2,3,4,5,6,7], k = 3
# Output: [5,6,7,1,2,3,4]
# Explanation:
# rotate 1 steps to the right: [7,1,2,3,4,5,6]
# rotate 2 steps to the right: [6,7,1,2,3,4,5]
# rotate 3 steps to the right: [5,6,7,1,2,3,4]

# Example 2:
# Input: nums = [-1,-100,3,99], k = 2
# Output: [3,99,-1,-100]
# Explanation:
# rotate 1 steps to the right: [99,-1,-100,3]
# rotate 2 steps to the right: [3,99,-1,-100]
class Solution(object):
    def rotate(self, nums, k):
        """
        :type nums: List[int]
        :type k: int
        :rtype: None Do not return anything, modify nums in-place instead.
        """
        ln=len(nums)
        nums2=nums[:ln-k]
        nums3=nums[ln-k:]
        final_nums=nums3+nums2
        return final_nums

nums=[1,2,3,4,5,6,7]
k=3
solution=Solution()
print(solution.rotate(nums,k))



class Solution3(object):
    def rotate(self, nums, k):
        """
        :type nums: List[int]
        :type k: int
        :rtype: None Do not return anything, modify nums in-place instead.
        """

        l = len(nums)
        k = k % l
        for i in range(0, (l - k) // 2):
            nums[i], nums[l - k - 1 - i] = nums[l - k - 1 - i], nums[i]
        for i in range(l - k, l - k // 2):
            nums[i], nums[2 * l - k - i - 1] = nums[2 * l - k - i - 1], nums[i]
        for i in range(0, l//2):
            nums[i], nums[l - 1 - i] = nums[l - 1 - i], nums[i]

class Solution4(object):
    def rotate(self, nums, k):
        """
        :type nums: List[int]
        :type k: int
        :rtype: None Do not return anything, modify nums in-place instead.
        """

        k = k % len(nums)
        nums[:] = nums[-k:] + nums[:-k]

# It's worst solution as time cmplexity will be around O(n**2). Loop will run n times and insert function will also displace list content by n times.
class Solution2(object):
    def rotate(self, nums, k):
        """
        :type nums: List[int]
        :type k: int
        :rtype: None Do not return anything, modify nums in-place instead.
        """
        if k == 0:
            return nums

        for i in range(k):
            last = nums.pop()
            nums.insert(0, last)
        return nums
