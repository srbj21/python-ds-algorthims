#You are a product manager and currently leading a team to develop a new product. Unfortunately, the latest version of your product fails
#the quality check. Since each version is developed based on the previous version, all the versions after a bad version are also bad.

#Suppose you have n versions [1, 2, ..., n] and you want to find out the first bad one, which causes all the following ones to be bad.
#You are given an API bool isBadVersion(version) which returns whether version is bad. Implement a function to find the first bad version. You should minimize the number of calls to the API.

#Example 1:
#Input: n = 5, bad = 4
#Output: 4

#Explanation:
#call isBadVersion(3) -> false
#call isBadVersion(5) -> true
#call isBadVersion(4) -> true
#Then 4 is the first bad version.

#Example 2:
#Input: n = 1, bad = 1
#Output: 1
#Time complexity : O(logn). The search space is halved each time, so the time complexity is O(\log n)O(logn).
#Space complexity : O(1)

#Approach #2 (Binary Search) [Accepted]
#It is not difficult to see that this could be solved using a classic algorithm - Binary search.
#Let us see how the search space could be halved each time below.

#Scenario #1: isBadVersion(mid) => false

 1 2 3 4 5 6 7 8 9
 G G G G G G B B B       G = Good, B = Bad
 |       |       |
left    mid    right

Scenario #2: isBadVersion(mid) => true

 1 2 3 4 5 6 7 8 9
 G G G B B B B B B       G = Good, B = Bad
 |       |       |
left    mid    right
#The only scenario left is where isBadVersion(mid) \Rightarrow trueisBadVersion(mid)⇒true. This tells us that midmid may or may not be
#the first bad version, but we can tell for sure that all versions after midmid can be discarded. Therefore we set right = midright=mid as the new search space of interval [left,mid][left,mid] (inclusive).

# The isBadVersion API is already defined for you.
# @param version, an integer
# @return a bool
# def isBadVersion(version):

class Solution(object):
    def firstBadVersion(self, n):
        high=n
        low=1
        while low < high:
            mid=low+(high-low)//2
            if isBadVersion(mid):
                high = mid
            else:
                low=mid+1
        return low
