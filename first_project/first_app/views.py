from django.shortcuts import render
from django.rest_framework import api_view
from django.http import JsonResponse

# Create your views here.
@api_view(['POST'])
def example(request):
    try:
        var = "Hello, World!"
        print(var)
        return JsonResponse(var)
    except Exception as e:
        print(f"Error in Getting Hello world : {str(e)}")