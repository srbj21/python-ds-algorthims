#Given a sorted array of distinct integers and a target value, return the index if the target is found.
#If not, return the index where it would be if it were inserted in order.
#You must write an algorithm with O(log n) runtime complexity.


#Example 1:
#Input: nums = [1,3,5,6], target = 5
#Output: 2
#Example 2:

#Input: nums = [1,3,5,6], target = 2
#Output: 1
#Example 3:

#Input: nums = [1,3,5,6], target = 7
#Output: 4
#Example 4:

#Input: nums = [1,3,5,6], target = 0
#Output: 0
#Example 5:

#Input: nums = [1], target = 0
#Output: 0

class Solution(object):
    def searchInsert(self, nums, target):
        low=0
        high=len(nums)-1
        while low <=high:
            mid=low+(high-low)//2
            if target==nums[mid]:
                return mid
            elif target<nums[mid]:
                high=mid-1
            else:
                low=mid+1
            print('high {},mid {}, low {}'.format(high,mid,low))
        return low

nums=[1,3,5,6]
target=2
solution=Solution()
print(solution.searchInsert(nums,target))
