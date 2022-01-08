# Given a 1-indexed array of integers numbers that is already sorted in non-decreasing order, find two numbers such that they add up to a specific target number. Let these two numbers be numbers[index1] and numbers[index2] where 1 <= index1 < index2 <= numbers.length.
# Return the indices of the two numbers, index1 and index2, added by one as an integer array [index1, index2] of length 2.
# The tests are generated such that there is exactly one solution. You may not use the same element twice.

#Example 1:
# Input: numbers = [2,7,11,15], target = 9
# Output: [1,2]
# Explanation: The sum of 2 and 7 is 9. Therefore, index1 = 1, index2 = 2. We return [1, 2].

# Example 2:
# Input: numbers = [2,3,4], target = 6
# Output: [1,3]
# Explanation: The sum of 2 and 4 is 6. Therefore index1 = 1, index2 = 3. We return [1, 3].

# Example 3:
# Input: numbers = [-1,0], target = -1
# Output: [1,2]
# Explanation: The sum of -1 and 0 is -1. Therefore index1 = 1, index2 = 2. We return [1, 2].

# This approach is O(n**2) appraoch as it involves two nested for loops.
class Solution(object):
    def twoSum(self, numbers, target):
        """
        :type numbers: List[int]
        :type target: int
        :rtype: List[int]
        """
        l1=0
        l2=0
        for i in range(len(numbers)):
            for j in range(len(numbers)):
                if i!=j and numbers[i]+numbers[j]==target:
                    l1=i
                    l2=j
                    break
            if i!=j and numbers[i]+numbers[j]==target:
                break

        return [l1+1,l2+1]

numbers= [5,25,75]
target=100

#solution=Solution()
#print(solution.twoSum(numbers,target))


# This approach is O(n) appraoch as it involves one while loop.
class Solution2(object):
    def twoSum(self, numbers, target):
        """
        :type numbers: List[int]
        :type target: int
        :rtype: List[int]
        """
        left,right=0,len(numbers)-1
        while(left<right):
            if numbers[left]+numbers[right]==target:
                return [left+1,right+1]
            elif numbers[left]+numbers[right]>target:
                right-=1
            else:
                left+=1

numbers= [5,25,75]
target=100

solution=Solution2()
print(solution.twoSum(numbers,target))
