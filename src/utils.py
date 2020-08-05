def find_intersect_area(r0, r1):
    ''' find intersection area, return None if they are not intersect'''
    left = max(r1["left"], r0["left"])
    right = min(r1["right"], r0["right"])
    bottom = min(r1["bottom"], r0["bottom"])
    top = max(r1["top"], r0["top"])

    if (left < right) and (top < bottom):
        return (right - left) * (bottom - top)
    else:
        return None
